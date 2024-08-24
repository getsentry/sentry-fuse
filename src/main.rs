use core::future::Future;
use libc::{ENOENT, ENOTDIR};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use clap::Parser;
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyOpen, Request, FUSE_ROOT_ID,
};
use reqwest::{header, Client, ClientBuilder, Response};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio_stream::{Stream, StreamExt};

const TTL: Duration = Duration::from_secs(1);

#[derive(Deserialize, Debug)]
struct Organization {
    slug: String,
    //lots more fields I don't care about (some of which are undocumented).
}

#[derive(Deserialize, Debug)]
struct Project {
    slug: String,
    //lots more fields I don't care about (some of which are undocumented).
}

#[derive(Deserialize, Debug)]
struct Issue {
    id: String,
    //title: String,
    //lots more fields I don't care about (some of which are undocumented).
}

#[derive(Deserialize, Debug)]
struct Event {
    id: String,
    //lots more fields I don't care about (some of which are undocumented).
}

fn parse_links<'a, I>(links: I) -> HashMap<String, HashMap<&'static str, String>>
where
    I: Iterator<Item = &'a header::HeaderValue>,
{
    let mut ret = HashMap::new();
    for link in links {
        let individuals = link.to_str().unwrap().split(',').map(|x| x.trim());
        for individual in individuals {
            let parts: Vec<_> = individual
                .split(';')
                .map(|x| x.trim().to_string())
                .collect();
            let [uri, params @ ..] = parts.as_slice() else {
                panic!("{:?}", parts)
            };
            let uri = uri
                .strip_prefix('<')
                .unwrap()
                .strip_suffix('>')
                .unwrap()
                .to_string();
            let mut params: HashMap<_, _> = params
                .iter()
                .map(|x| {
                    let pair = x.split('=').collect::<Vec<_>>();
                    let [a, b] = pair.as_slice() else {
                        panic!("{:?}", pair)
                    };
                    (
                        a.to_string(),
                        b.strip_prefix('"')
                            .unwrap()
                            .strip_suffix('"')
                            .unwrap()
                            .to_string(),
                    )
                })
                .collect();
            ret.insert(
                params.remove("rel").unwrap(),
                HashMap::from([
                    ("uri", uri),
                    ("results", params.remove("results").unwrap()),
                    ("cursor", params.remove("cursor").unwrap()),
                ]),
            );
        }
    }
    ret
}

struct ApiList<'a, T> {
    client: &'a Client,
    uri: Option<String>,
    next_uri: Option<String>,
    current_page: std::vec::IntoIter<T>,
    resp_future: Option<Pin<Box<dyn Future<Output = Result<Response, reqwest::Error>>>>>,
    json_future: Option<Pin<Box<dyn Future<Output = reqwest::Result<Vec<T>>>>>>,
}

impl<'a, T> ApiList<'a, T>
where
    T: DeserializeOwned,
{
    async fn new(client: &'a Client, uri: String) -> ApiList<'a, T> {
        let resp = client.get(uri).send().await.unwrap();
        let mut links = parse_links(resp.headers().get_all(header::LINK).iter());
        let uri = if links["next"]["results"] == "true" {
            links.remove("next").unwrap().remove("uri")
        } else {
            None
        };
        ApiList {
            client,
            uri,
            next_uri: None,
            current_page: resp.json::<Vec<T>>().await.unwrap().into_iter(),
            resp_future: None,
            json_future: None,
        }
    }
}

impl<'a, T> Unpin for ApiList<'a, T> {}

impl<'a, T> Stream for ApiList<'a, T>
where
    T: DeserializeOwned + 'static,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        match self.current_page.next() {
            Some(y) => Poll::Ready(Some(y)),
            None => match &self.uri {
                Some(y) => {
                    if self.json_future.is_none() {
                        if self.resp_future.is_none() {
                            self.resp_future = Some(Box::pin(self.client.get(y).send()));
                        }
                        match Pin::new(&mut self.resp_future.as_mut().unwrap()).poll(cx) {
                            Poll::Ready(result) => {
                                let resp = result.unwrap();
                                let mut links =
                                    parse_links(resp.headers().get_all(header::LINK).iter());
                                self.next_uri = if links["next"]["results"] == "true" {
                                    links.remove("next").unwrap().remove("uri")
                                } else {
                                    None
                                };
                                self.json_future = Some(Box::pin(resp.json::<Vec<T>>()))
                            }
                            Poll::Pending => return Poll::Pending,
                        }
                    }
                    match Pin::new(&mut self.json_future.as_mut().unwrap()).poll(cx) {
                        Poll::Ready(result) => {
                            self.current_page = result.unwrap().into_iter();
                            self.uri = self.next_uri.take();
                            self.resp_future = None;
                            self.json_future = None;
                            Poll::Ready(self.current_page.next())
                        }
                        Poll::Pending => Poll::Pending,
                    }
                }
                None => Poll::Ready(None),
            },
        }
    }
}

#[derive(Debug)]
enum ApiObjectParts {
    Root,
    Organization,
    Project(String),
    Issue(String, String),
    Event(String, String, String),
}

#[derive(Debug)]
struct SentryFSInfo {
    parent: u64,
    name: String,
    parts: ApiObjectParts,
    kind: FileType,
    size: u64,
    //name -> (inode, type, size)
    children: Option<HashMap<String, (u64, FileType, u64)>>,
    data: Option<Bytes>,
}

struct SentryFS {
    runtime: Runtime,
    client: Client,
    inode_map: HashMap<u64, SentryFSInfo>,
    last_used_inode: u64,
}

impl SentryFS {
    fn new(runtime: Runtime, client: Client) -> SentryFS {
        let mut inode_map = HashMap::new();
        inode_map.insert(
            FUSE_ROOT_ID,
            SentryFSInfo {
                parent: 0,
                name: "/".to_string(),
                parts: ApiObjectParts::Root,
                kind: FileType::Directory,
                size: 0,
                children: None,
                data: None,
            },
        );
        SentryFS {
            runtime,
            client,
            inode_map,
            last_used_inode: FUSE_ROOT_ID,
        }
    }
}

impl Filesystem for SentryFS {
    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let info = match self.inode_map.get(&ino) {
            Some(x) => x,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        if info.children.is_none() {
            match &info.parts {
                ApiObjectParts::Root => {
                    self.runtime.block_on(async {
                        let mut org_stream = ApiList::<Organization>::new(
                            &self.client,
                            "https://sentry.io/api/0/organizations/".to_string(),
                        )
                        .await;
                        let mut children = HashMap::new();
                        while let Some(org) = org_stream.next().await {
                            self.last_used_inode += 1;
                            children.insert(
                                org.slug.clone(),
                                (self.last_used_inode, FileType::Directory, 0),
                            );
                            self.inode_map.insert(
                                self.last_used_inode,
                                SentryFSInfo {
                                    parent: ino,
                                    name: org.slug,
                                    parts: ApiObjectParts::Organization,
                                    kind: FileType::Directory,
                                    size: 0,
                                    children: None,
                                    data: None,
                                },
                            );
                        }
                        self.inode_map.get_mut(&ino).unwrap().size = children.len() as u64;
                        self.inode_map.get_mut(&ino).unwrap().children = Some(children);
                    });
                }
                ApiObjectParts::Organization => {
                    let org = info.name.clone();
                    self.runtime.block_on(async {
                        let mut proj_stream = ApiList::<Project>::new(
                            &self.client,
                            format!("https://sentry.io/api/0/organizations/{}/projects/", org),
                        )
                        .await;
                        let mut children = HashMap::new();
                        while let Some(proj) = proj_stream.next().await {
                            self.last_used_inode += 1;
                            children.insert(
                                proj.slug.clone(),
                                (self.last_used_inode, FileType::Directory, 0),
                            );
                            self.inode_map.insert(
                                self.last_used_inode,
                                SentryFSInfo {
                                    parent: ino,
                                    name: proj.slug,
                                    parts: ApiObjectParts::Project(org.clone()),
                                    kind: FileType::Directory,
                                    size: 0,
                                    children: None,
                                    data: None,
                                },
                            );
                        }
                        self.inode_map.get_mut(&ino).unwrap().size = children.len() as u64;
                        let parent_ino = self.inode_map.get(&ino).unwrap().parent;
                        self.inode_map
                            .get_mut(&parent_ino)
                            .unwrap()
                            .children
                            .as_mut()
                            .unwrap()
                            .get_mut(&org)
                            .unwrap()
                            .2 = children.len() as u64;
                        self.inode_map.get_mut(&ino).unwrap().children = Some(children);
                    });
                }
                ApiObjectParts::Project(org) => {
                    let org = org.clone();
                    let proj = info.name.clone();
                    self.runtime.block_on(async {
                        let mut issue_stream = ApiList::<Issue>::new(
                            &self.client,
                            format!("https://sentry.io/api/0/projects/{}/{}/issues/", org, proj),
                        )
                        .await;
                        let mut children = HashMap::new();
                        while let Some(issue) = issue_stream.next().await {
                            self.last_used_inode += 1;
                            children.insert(
                                issue.id.clone(),
                                (self.last_used_inode, FileType::Directory, 0),
                            );
                            self.inode_map.insert(
                                self.last_used_inode,
                                SentryFSInfo {
                                    parent: ino,
                                    name: issue.id,
                                    parts: ApiObjectParts::Issue(org.clone(), proj.clone()),
                                    kind: FileType::Directory,
                                    size: 0,
                                    children: None,
                                    data: None,
                                },
                            );
                        }
                        self.inode_map.get_mut(&ino).unwrap().size = children.len() as u64;
                        let parent_ino = self.inode_map.get(&ino).unwrap().parent;
                        self.inode_map
                            .get_mut(&parent_ino)
                            .unwrap()
                            .children
                            .as_mut()
                            .unwrap()
                            .get_mut(&proj)
                            .unwrap()
                            .2 = children.len() as u64;
                        self.inode_map.get_mut(&ino).unwrap().children = Some(children);
                    });
                }
                ApiObjectParts::Issue(org, proj) => {
                    let org = org.clone();
                    let proj = proj.clone();
                    let issue = info.name.clone();
                    self.runtime.block_on(async {
                        let mut event_stream = ApiList::<Event>::new(
                            &self.client,
                            format!(
                                "https://sentry.io/api/0/organizations/{}/issues/{}/events/",
                                org, issue
                            ),
                        )
                        .await;
                        let mut children = HashMap::new();
                        while let Some(event) = event_stream.next().await {
                            self.last_used_inode += 1;
                            children.insert(
                                event.id.clone(),
                                (self.last_used_inode, FileType::RegularFile, 0),
                            );
                            self.inode_map.insert(
                                self.last_used_inode,
                                SentryFSInfo {
                                    parent: ino,
                                    name: event.id,
                                    parts: ApiObjectParts::Event(
                                        org.clone(),
                                        proj.clone(),
                                        issue.clone(),
                                    ),
                                    kind: FileType::RegularFile,
                                    size: 0,
                                    children: None,
                                    data: None,
                                },
                            );
                        }
                        self.inode_map.get_mut(&ino).unwrap().size = children.len() as u64;
                        let parent_ino = self.inode_map.get(&ino).unwrap().parent;
                        self.inode_map
                            .get_mut(&parent_ino)
                            .unwrap()
                            .children
                            .as_mut()
                            .unwrap()
                            .get_mut(&issue)
                            .unwrap()
                            .2 = children.len() as u64;
                        self.inode_map.get_mut(&ino).unwrap().children = Some(children);
                    });
                }
                ApiObjectParts::Event(_org, _proj, _issue) => {
                    reply.error(ENOTDIR);
                    return;
                }
            }
        }
        reply.opened(0, 0);
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_info = match self.inode_map.get(&parent) {
            Some(x) => x,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        if parent_info.children.is_none() {
            reply.error(ENOENT);
            return;
        }
        let (ino, kind, size) = match parent_info
            .children
            .as_ref()
            .unwrap()
            .get(name.to_str().unwrap())
            .copied()
        {
            Some(v) => v,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        reply.entry(
            &TTL,
            &FileAttr {
                ino,
                size,
                blocks: 0,
                atime: UNIX_EPOCH, // 1970-01-01 00:00:00
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind,
                perm: 0o755,
                nlink: 2,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: 512,
            },
            0,
        );
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let info = match self.inode_map.get(&ino) {
            Some(x) => x,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        reply.attr(
            &TTL,
            &FileAttr {
                ino,
                size: info.size,
                blocks: 0,
                atime: UNIX_EPOCH, // 1970-01-01 00:00:00
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: info.kind,
                perm: 0o755,
                nlink: 2,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: 512,
            },
        );
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let info = match self.inode_map.get(&ino) {
            Some(x) => x,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        for (i, entry) in info
            .children
            .as_ref()
            .unwrap()
            .iter()
            .enumerate()
            .skip(offset as usize)
        {
            // i + 1 means the offset of the next entry
            if reply.add(entry.1 .0, (i + 1) as i64, entry.1 .1, &entry.0) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let info = match self.inode_map.get_mut(&ino) {
            Some(x) => x,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        match info.kind {
            FileType::Directory => {
                reply.error(ENOENT);
            }
            FileType::RegularFile => {
                if info.data.is_none() {
                    self.runtime.block_on(async {
                        if let ApiObjectParts::Event(org, proj, _issue) = &info.parts {
                            info.data = Some(
                                self.client
                                    .get(format!(
                                        "https://sentry.io/api/0/projects/{}/{}/events/{}/",
                                        org, proj, info.name
                                    ))
                                    .send()
                                    .await
                                    .unwrap()
                                    .bytes()
                                    .await
                                    .unwrap(),
                            );
                        } else {
                            panic!();
                        }
                    });
                }
                info.size = info.data.as_mut().unwrap().len() as u64;
                let parent_ino = info.parent;
                let name = info.name.clone();
                self.inode_map
                    .get_mut(&parent_ino)
                    .unwrap()
                    .children
                    .as_mut()
                    .unwrap()
                    .get_mut(&name)
                    .unwrap()
                    .2 = info.data.as_mut().unwrap().len() as u64;
                reply.opened(0, 0);
            }
            _ => panic!(),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let info = match self.inode_map.get(&ino) {
            Some(x) => x,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        if offset as usize >= info.data.as_ref().unwrap().len() {
            reply.data(b"");
        } else {
            reply.data(
                &info.data.as_ref().unwrap()[offset as usize..(offset as usize) + (size as usize)],
            );
        }
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    mount_point: String,

    #[arg(short, long)]
    token_path: String,
}

fn main() {
    let args = Args::parse();
    println!(
        "mount_point: {}\ntoken_path: {}",
        args.mount_point, args.token_path
    );
    let mount_options = vec![MountOption::RO, MountOption::FSName("sentry".to_string())];
    let mut token: header::HeaderValue = format!(
        "Bearer {}",
        fs::read_to_string(args.token_path).unwrap().trim()
    )
    .parse()
    .unwrap();
    token.set_sensitive(true);
    let mut auth_header = header::HeaderMap::new();
    auth_header.insert(header::AUTHORIZATION, token);
    let client = ClientBuilder::new()
        .default_headers(auth_header)
        .gzip(true)
        .referer(false)
        .https_only(true)
        .build()
        .unwrap();
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let sentry_fs = SentryFS::new(runtime, client);
    fuser::mount2(sentry_fs, args.mount_point, &mount_options).unwrap();
}
