use clap_port_flag::Port;
use futures::prelude::*;
use hyper::service::service_fn;
use hyper::{Body, Response, Request};
use clap::Parser;

#[derive(Debug, Parser)]
struct Cli {
    #[clap(flatten)]
    port: Port,
}

async fn hello(_: Request<Body>) -> Result<Response<String>, std::convert::Infallible> {
    Ok(Response::new(String::from("Hello World!")))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    let listener = args.port.bind()?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
    let addr = listener.local_addr()?;

    println!("Server listening on {}", addr);

    let (stream, _) = listener.accept().await?;
    if let Err(e) = hyper::server::conn::Http::new()
        .serve_connection(stream, service_fn(hello))
        .await
    {
        eprintln!("server error: {}", e);
    }
    Ok(())
}