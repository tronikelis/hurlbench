use std::{
    cmp,
    collections::BinaryHeap,
    env, fmt, fs,
    io::{self, Write},
    sync, thread, time,
};

use anyhow::{Result, anyhow};
use hurl_core::error::DisplaySourceError;

const USAGE: &str = "
USAGE:
    hurlbench [OPTIONS] <FILEPATH>

ARGS:
    <FILEPATH>    Path to the input file

OPTIONS:
    -d, --duration <DURATION>      Duration with unit suffix:
                                  s = seconds, m = milliseconds
                                  [default: 10s]

    -p, --parallelism <N>          Number of parallel workers
                                  [default: 1]
";

#[derive(Debug, Clone)]
struct Endpoint {
    url: String,
    headers: Vec<(String, String)>,
    method: String,
    body: Option<()>,
}

fn resolve_template(template: &hurl_core::ast::Template) -> String {
    let mut string = String::new();

    for element in &template.elements {
        match element {
            hurl_core::ast::TemplateElement::String {
                value,
                source: _source,
            } => string.push_str(&value),
            hurl_core::ast::TemplateElement::Placeholder(_placeholder) => {
                todo!("placeholders not supported yet");
            }
        }
    }

    string
}

impl Endpoint {
    fn new(entry: &hurl_core::ast::Entry) -> Self {
        let url = resolve_template(&entry.request.url);
        let headers = entry
            .request
            .headers
            .iter()
            .map(|key_value| {
                (
                    resolve_template(&key_value.key),
                    resolve_template(&key_value.value),
                )
            })
            .collect();

        Self {
            url,
            headers,
            body: None,
            method: entry.request.method.to_string(),
        }
    }

    fn create_header_list(&self) -> Result<curl::easy::List> {
        let mut list = curl::easy::List::new();
        for (key, value) in &self.headers {
            list.append(&format!("{}: {}", key, value))?;
        }
        Ok(list)
    }

    fn send_request(&self, client: &mut curl::easy::Easy) -> Result<()> {
        client.url(&self.url)?;
        client.http_headers(self.create_header_list()?)?;
        match self.method.as_str() {
            "GET" => client.get(true),
            "POST" => client.post(true),
            "PUT" => client.put(true),
            _ => todo!("unknown method handle"),
        }?;

        client.perform()?;

        Ok(())
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let body = match &self.body {
            Some(_) => "body",
            None => "no body",
        };
        write!(
            f,
            "{}: {}, {} headers, {}",
            &self.method,
            &self.url,
            self.headers.len(),
            body
        )
    }
}

struct CmdArgs {
    parrallelism: usize,
    duration: time::Duration,
    filepath: String,
}

impl CmdArgs {
    fn new() -> Result<Self> {
        let mut duration = None;
        let mut parallelism = None;
        let mut filepath = None;

        let missing_argument = || anyhow!("missing argument");
        let mut args = env::args().skip(1);
        loop {
            let Some(arg) = args.next() else {
                break;
            };
            match arg.as_str() {
                "-d" | "--duration" => {
                    let duration_string = args.next().ok_or_else(missing_argument)?;
                    let duration_int: usize =
                        duration_string[..duration_string.len() - 1].parse()?;
                    duration = Some(
                        match duration_string
                            .as_bytes()
                            .last()
                            .ok_or_else(missing_argument)?
                            .to_ascii_lowercase()
                        {
                            b's' => time::Duration::from_secs(duration_int as u64),
                            b'm' => time::Duration::from_millis(duration_int as u64),
                            v => {
                                return Err(anyhow!(
                                    "unknown time modifier {}, expected s/m",
                                    v as char
                                ));
                            }
                        },
                    );
                }
                "-p" | "--parallelism" => {
                    parallelism = Some(args.next().ok_or_else(missing_argument)?.parse()?);
                }
                v => filepath = Some(v.to_string()),
            };
        }

        Ok(Self {
            duration: duration.unwrap_or(time::Duration::from_secs(10)),
            parrallelism: parallelism.unwrap_or(1),
            filepath: filepath.ok_or_else(missing_argument)?,
        })
    }
}

impl fmt::Display for CmdArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "filepath: {}, duration_s: {:.1}, parallelism: {}",
            &self.filepath,
            self.duration.as_secs_f32(),
            self.parrallelism
        )
    }
}

struct Statistics {
    max_duration: BinaryHeap<time::Duration>,
    min_duration: BinaryHeap<cmp::Reverse<time::Duration>>,
}

impl Statistics {
    fn new() -> Self {
        Self {
            max_duration: BinaryHeap::new(),
            min_duration: BinaryHeap::new(),
        }
    }

    fn request_count(&self) -> usize {
        self.max_duration.len()
    }

    fn track(&mut self, duration: time::Duration) {
        self.max_duration.push(duration);
        self.min_duration.push(cmp::Reverse(duration));
    }

    fn get_max_duration(&self) -> Option<time::Duration> {
        self.max_duration.peek().map(|v| v.clone())
    }

    fn get_min_duration(&self) -> Option<time::Duration> {
        self.min_duration.peek().map(|v| v.0.clone())
    }

    // 99
    // 99.9
    // 50
    fn p(&self, value: f32) -> Option<time::Duration> {
        let index = (self.max_duration.len() as f32 * (1.0 - value / 100.0)) as usize;
        let vec = self.max_duration.clone().into_sorted_vec();
        if vec.len() == 0 {
            return None;
        }
        vec.get(vec.len() - index - 1).map(|v| v.clone())
    }
}

impl fmt::Display for Statistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let default_duration = time::Duration::from_secs(0);
        write!(
            f,
            "max: {:.4}s, min: {:.4}s, p99.9: {:.4}s, p99: {:.4}s, p95: {:.4}s, p50: {:.4}s",
            self.get_max_duration()
                .unwrap_or(default_duration)
                .as_secs_f32(),
            self.get_min_duration()
                .unwrap_or(default_duration)
                .as_secs_f32(),
            self.p(99.9).unwrap_or(default_duration).as_secs_f32(),
            self.p(99.0).unwrap_or(default_duration).as_secs_f32(),
            self.p(95.0).unwrap_or(default_duration).as_secs_f32(),
            self.p(50.0).unwrap_or(default_duration).as_secs_f32(),
        )
    }
}

fn main() -> Result<()> {
    curl::init();

    let cmd_args = match CmdArgs::new() {
        Err(err) => {
            eprintln!("{}", USAGE);
            Err(err)
        }
        v => v,
    }?;
    eprintln!("{}", cmd_args);

    let file_contents = fs::read_to_string(&cmd_args.filepath)?;
    let hurl_file = hurl_core::parser::parse_hurl_file(&file_contents).map_err(|v| {
        anyhow!(
            v.message(&file_contents.lines().collect::<Vec<_>>())
                .to_string(hurl_core::text::Format::Plain)
        )
    })?;

    let endpoint = Endpoint::new(
        hurl_file
            .entries
            .get(0)
            .ok_or_else(|| anyhow!("expected hurl file to have an entry"))?,
    );
    eprintln!("endpoint: {}", &endpoint);

    let (request_tx, request_rx) = sync::mpsc::channel::<Result<time::Duration>>();

    let mut thread_handles = Vec::new();
    for _ in 0..cmd_args.parrallelism {
        thread_handles.push(thread::spawn({
            let endpoint = endpoint.clone();
            let request_tx = request_tx.clone();
            move || -> Result<()> {
                let mut client = curl::easy::Easy::new();
                loop {
                    let now = time::Instant::now();
                    let duration = match endpoint.send_request(&mut client) {
                        Ok(_) => Ok(now.elapsed()),
                        Err(err) => Err(err),
                    };
                    if let Err(_) = request_tx.send(duration) {
                        break;
                    }
                }
                Ok(())
            }
        }));
    }

    let statistics = sync::Arc::new(sync::Mutex::new(Statistics::new()));
    let start_instant = time::Instant::now();

    thread::spawn({
        let statistics = statistics.clone();
        move || -> Result<()> {
            let mut stderr = io::stderr();
            write!(stderr, "\x1b[s")?;

            let mut prev_request_count: usize = 0;
            let mut prev_instant = time::Instant::now();
            loop {
                write!(stderr, "\x1b[u\x1b[0J")?;
                let current_request_count = statistics.lock().unwrap().request_count();
                let rps = ((current_request_count - prev_request_count) as f32)
                    / prev_instant.elapsed().as_secs_f32();

                prev_instant = time::Instant::now();
                prev_request_count = current_request_count;
                write!(
                    stderr,
                    "({:.1}/{:.1}) [{}rps] {}",
                    start_instant.elapsed().as_secs_f32(),
                    cmd_args.duration.as_secs_f32(),
                    rps as usize,
                    statistics.lock().unwrap()
                )?;
                thread::sleep(time::Duration::from_secs(1));
            }
        }
    });

    loop {
        if start_instant.elapsed() > cmd_args.duration {
            break;
        }
        let request = request_rx.recv()??;
        statistics.lock().unwrap().track(request);
    }
    drop(request_rx);

    eprintln!("\n{}", statistics.lock().unwrap());
    eprintln!("waiting for threads to settle");
    for thread_handle in thread_handles {
        thread_handle.join().unwrap()?;
    }

    Ok(())
}
