use std::net::TcpStream;
use crate::error::{Error, Result};
use crate::server::{Request, Response};
use crate::sql::engine::{Mode, Status};
use crate::sql::execution::ResultSet;
use crate::sql::schema::Table;

use futures::future::FutureExt as _;
use futures::sink::SinkExt as _;
use futures::stream::TryStreamExt as _;
use rand::Rng as _;
use std::cell::Cell;
use std::future::Future;
use std::ops::{Deref, Drop};
use std::sync::Arc;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{Mutex, MutexGuard};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

type Connection = tokio_serde::Framed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Result<Response>,
    Request,
    tokio_serde::formats::Bincode<Result<Response>, Request>,
>;