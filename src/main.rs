use std::{
    cmp,
    collections::BinaryHeap,
    env, fmt, fs,
    io::{self, Read, Write},
    ptr, sync, thread, time,
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

    -v, --variable <KEY>=<VALUE>   Pass variables to be expanded
";

#[derive(Debug, Clone)]
struct Variable {
    key: String,
    value: String,
}

#[derive(Debug)]
struct ByteReader<'a> {
    index: usize,
    slice: &'a [u8],
}

impl<'a> ByteReader<'a> {
    fn new(slice: &'a [u8]) -> Self {
        Self { slice, index: 0 }
    }
}

impl<'a> io::Read for ByteReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.index >= self.slice.len() {
            return Ok(0);
        }

        let copied = self.slice.len().min(buf.len());
        unsafe { ptr::copy_nonoverlapping(self.slice.as_ptr(), buf.as_mut_ptr(), copied) };
        self.index += buf.len();
        Ok(copied)
    }
}

#[derive(Debug, Clone)]
struct Endpoint {
    url: String,
    headers: Vec<(String, String)>,
    method: String,
    body: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct TemplateResolver {
    variables: Vec<Variable>,
}

impl TemplateResolver {
    fn new(variables: Vec<Variable>) -> Self {
        Self { variables }
    }

    fn resolve_placeholder(&self, placeholder: &hurl_core::ast::Placeholder) -> Result<String> {
        match &placeholder.expr.kind {
            hurl_core::ast::ExprKind::Variable(variable) => self
                .variables
                .iter()
                .find(|v| &v.key == &variable.name)
                .map(|v| v.value.clone())
                .ok_or_else(|| anyhow!("variable: {} not found", &variable.name)),
            _ => Err(anyhow!("this expression type is not supported")),
        }
    }

    fn resolve(&self, template: &hurl_core::ast::Template) -> Result<String> {
        let mut string = String::new();

        for element in &template.elements {
            match element {
                hurl_core::ast::TemplateElement::String {
                    value,
                    source: _source,
                } => string.push_str(value),
                hurl_core::ast::TemplateElement::Placeholder(placeholder) => {
                    string.push_str(&self.resolve_placeholder(placeholder)?);
                }
            }
        }

        Ok(string)
    }
}

#[derive(Debug)]
struct JsonResolver {
    template_resolver: TemplateResolver,
}

impl JsonResolver {
    fn new(template_resolver: TemplateResolver) -> Self {
        Self { template_resolver }
    }

    fn quote(string: &str) -> String {
        format!("\"{}\"", string.replace("\"", "\\\""))
    }

    fn resolve(&self, json_value: &hurl_core::ast::JsonValue) -> Result<String> {
        Ok(match json_value {
            hurl_core::ast::JsonValue::Null => "null".to_string(),
            hurl_core::ast::JsonValue::Number(v) => v.clone(),
            hurl_core::ast::JsonValue::Boolean(bool) => {
                (if *bool { "true" } else { "false" }).to_string()
            }
            hurl_core::ast::JsonValue::String(template) => {
                Self::quote(&self.template_resolver.resolve(template)?)
            }
            hurl_core::ast::JsonValue::Placeholder(placeholder) => {
                self.template_resolver.resolve_placeholder(placeholder)?
            }
            hurl_core::ast::JsonValue::List {
                space0: _space0,
                elements,
            } => {
                let mut string = "[".to_string();
                for (index, element) in elements.iter().enumerate() {
                    string.push_str(&self.resolve(&element.value)?);
                    if index != elements.len() - 1 {
                        string.push(',');
                    }
                }
                string.push(']');
                string
            }
            hurl_core::ast::JsonValue::Object {
                space0: _space0,
                elements,
            } => {
                let mut string = "{".to_string();
                for (index, element) in elements.iter().enumerate() {
                    string.push_str(&Self::quote(
                        &self.template_resolver.resolve(&element.name)?,
                    ));
                    string.push(':');
                    string.push_str(&self.resolve(&element.value)?);
                    if index != elements.len() - 1 {
                        string.push(',');
                    }
                }
                string.push('}');
                string
            }
        })
    }
}

impl Endpoint {
    fn new(
        template_resolver: &TemplateResolver,
        json_resolver: &JsonResolver,
        entry: &hurl_core::ast::Entry,
    ) -> Result<Self> {
        let url = template_resolver.resolve(&entry.request.url)?;
        let headers = entry
            .request
            .headers
            .iter()
            .map(|key_value| {
                Ok((
                    template_resolver.resolve(&key_value.key)?,
                    template_resolver.resolve(&key_value.value)?,
                ))
            })
            .collect::<Result<_>>()?;

        let body = entry
            .request
            .body
            .as_ref()
            .map(|body| match &body.value {
                hurl_core::ast::Bytes::Json(json_value) => {
                    let result = json_resolver.resolve(json_value)?;
                    Ok(result.into_bytes())
                }
                _ => Err(anyhow!("this body is not yet supported")),
            })
            .transpose()?;

        Ok(Self {
            url,
            headers,
            body,
            method: entry.request.method.to_string(),
        })
    }

    fn create_header_list(&self) -> Result<curl::easy::List> {
        let mut list = curl::easy::List::new();
        for (key, value) in &self.headers {
            list.append(&format!("{}: {}", key, value))?;
        }
        Ok(list)
    }

    fn send_request(&self, client: &mut curl::easy::Easy) -> Result<u32> {
        client.reset();
        client.url(&self.url)?;
        client.http_headers(self.create_header_list()?)?;
        match self.method.as_str() {
            "GET" => client.get(true),
            "POST" => client.post(true),
            "PUT" => client.put(true),
            _ => todo!("unknown method handle"),
        }?;

        {
            let mut transfer = client.transfer();
            if let Some(body) = &self.body {
                let mut reader = ByteReader::new(body);
                transfer.read_function(move |data| Ok(reader.read(data).unwrap()))?;
                transfer.write_function(|data| {
                    // noop
                    Ok(data.len())
                })?;
            }
            transfer.perform()?;
        }
        Ok(client.response_code()?)
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
    variables: Vec<Variable>,
}

impl CmdArgs {
    fn new() -> Result<Self> {
        let mut duration = None;
        let mut parallelism = None;
        let mut filepath = None;
        let mut variables = Vec::new();

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
                "-v" | "--variable" => {
                    let definition = args.next().ok_or_else(missing_argument)?;
                    let mut by_equals = definition.split("=");
                    let key = by_equals.next().ok_or_else(missing_argument)?;
                    let value = by_equals.next().ok_or_else(missing_argument)?;
                    variables.push(Variable {
                        key: key.to_string(),
                        value: value.to_string(),
                    });
                }
                v => filepath = Some(v.to_string()),
            };
        }

        Ok(Self {
            duration: duration.unwrap_or(time::Duration::from_secs(10)),
            parrallelism: parallelism.unwrap_or(1),
            filepath: filepath.ok_or_else(missing_argument)?,
            variables,
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

#[derive(Debug)]
struct PLatencyBucket {
    count: usize,
    max_ms: f32,
}

#[derive(Debug)]
struct PLatency {
    buckets: Vec<PLatencyBucket>,
    total: usize,
}

impl PLatency {
    fn new(bucket_count: usize, granularity: f32, exp: f32) -> Self {
        Self {
            buckets: (1..=bucket_count)
                .map(|i| PLatencyBucket {
                    count: 0,
                    max_ms: (i as f32 * granularity) * exp.powf(i as f32),
                })
                .collect(),
            total: 0,
        }
    }

    fn track(&mut self, duration: time::Duration) {
        self.total += 1;

        let ms = duration.as_secs_f32() * 1000.0;
        for bucket in self.buckets.iter_mut().rev() {
            if bucket.max_ms <= ms {
                bucket.count += 1;
                return;
            }
        }

        if let Some(bucket) = self.buckets.first_mut() {
            bucket.count += 1;
        }
    }

    fn p_ms(&self, percent: f32) -> f32 {
        let mut count = 0;
        for (i, bucket) in self.buckets.iter().enumerate() {
            count += bucket.count;
            if ((count as f32 / self.total as f32) * 100.0) >= percent {
                if i == 0 {
                    return bucket.max_ms;
                }

                let last_bucket = self.buckets.get(i - 1).unwrap();
                return (last_bucket.max_ms + bucket.max_ms) / 2.0;
            }
        }

        0.0
    }
}

#[derive(Debug)]
struct RequestStatusCount(Vec<(usize, usize)>);

impl RequestStatusCount {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn track(&mut self, status: usize) {
        for count in self.0.iter_mut() {
            if count.0 == status {
                count.1 += 1;
                return;
            }
        }

        self.0.push((status, 1));
        self.0.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    }
}

impl fmt::Display for RequestStatusCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut string = String::new();
        for v in &self.0 {
            string.push_str(&format!("{}: {}\n", v.0, v.1));
        }
        write!(f, "{}", string)
    }
}

#[derive(Debug)]
struct Statistics {
    max_duration: BinaryHeap<time::Duration>,
    min_duration: BinaryHeap<cmp::Reverse<time::Duration>>,
    platency: PLatency,
    request_status_count: RequestStatusCount,
}

impl Statistics {
    fn new() -> Self {
        Self {
            max_duration: BinaryHeap::new(),
            min_duration: BinaryHeap::new(),
            platency: PLatency::new(800, 0.5, 1.005),
            request_status_count: RequestStatusCount::new(),
        }
    }

    fn request_count(&self) -> usize {
        self.max_duration.len()
    }

    fn track(&mut self, response: Response) {
        self.max_duration.push(response.duration);
        self.min_duration.push(cmp::Reverse(response.duration));
        self.platency.track(response.duration);
        self.request_status_count.track(response.status);
    }

    fn get_max_duration(&self) -> Option<time::Duration> {
        self.max_duration.peek().map(|v| v.clone())
    }

    fn get_min_duration(&self) -> Option<time::Duration> {
        self.min_duration.peek().map(|v| v.0.clone())
    }
}

impl fmt::Display for Statistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let default_duration = time::Duration::from_secs(0);
        write!(
            f,
            "max: {:.4}s\nmin: {:.4}s\np99.9: {:.4}s\np99: {:.4}s\np95: {:.4}s\np50: {:.4}s\nstatuses:\n{}",
            self.get_max_duration()
                .unwrap_or(default_duration)
                .as_secs_f32(),
            self.get_min_duration()
                .unwrap_or(default_duration)
                .as_secs_f32(),
            self.platency.p_ms(99.9) / 1000.0,
            self.platency.p_ms(99.0) / 1000.0,
            self.platency.p_ms(95.0) / 1000.0,
            self.platency.p_ms(50.0) / 1000.0,
            self.request_status_count,
        )
    }
}

struct Response {
    duration: time::Duration,
    status: usize,
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

    let template_resolver = TemplateResolver::new(cmd_args.variables.clone());
    let json_resolver = JsonResolver::new(template_resolver.clone());

    let endpoint = Endpoint::new(
        &template_resolver,
        &json_resolver,
        hurl_file
            .entries
            .get(0)
            .ok_or_else(|| anyhow!("expected hurl file to have an entry"))?,
    )?;
    eprintln!("endpoint: {}", &endpoint);

    let (response_tx, response_rx) = sync::mpsc::channel::<Result<Response>>();

    let mut thread_handles = Vec::new();
    for _ in 0..cmd_args.parrallelism {
        thread_handles.push(thread::spawn({
            let endpoint = endpoint.clone();
            let response_tx = response_tx.clone();
            move || -> Result<()> {
                let mut client = curl::easy::Easy::new();
                loop {
                    let now = time::Instant::now();
                    let response = endpoint.send_request(&mut client).map(|v| Response {
                        status: v as usize,
                        duration: now.elapsed(),
                    });
                    if let Err(_) = response_tx.send(response) {
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
                    "({:.1}/{:.1}) [{}rps]\n{}\n",
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
        let response = response_rx.recv()??;
        statistics.lock().unwrap().track(response);
    }
    drop(response_rx);

    eprintln!("\n{}", statistics.lock().unwrap());
    eprintln!("waiting for threads to settle");
    for thread_handle in thread_handles {
        thread_handle.join().unwrap()?;
    }

    Ok(())
}
