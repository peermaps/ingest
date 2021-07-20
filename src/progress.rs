use std::collections::{HashMap,VecDeque};

pub struct Progress {
  pub error_size: usize,
  pub sample_size: usize,
  pub stages: Vec<String>,
  pub info: HashMap<String,Info>,
}

impl std::fmt::Display for Progress {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    for s in self.stages.iter() {
      write![f, "{}\n", &self.info[s]]?;
    }
    Ok(())
  }
}

pub struct Info {
  label: String,
  start: Option<std::time::Instant>,
  end: Option<std::time::Instant>,
  samples: VecDeque<(std::time::Duration,u64)>,
  count: u64,
  sample_size: usize,
}

impl std::fmt::Display for Info {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let d = hms(match (self.start,self.end) {
      (Some(s),Some(e)) => e.checked_duration_since(s),
      (Some(s),None) => Some(s.elapsed()),
      _ => None
    });
    if let (Some(first),Some(last)) = (self.samples.front(),self.samples.back()) {
      let e = first.0.as_secs_f64() - last.0.as_secs_f64();
      let rate = ((first.1 - last.1) as f64) / e;
      write![f, "[{} {}] {:>10} ({:>6.0}/s)", self.label, d, self.count, rate]
    } else {
      write![f, "[{} {}] {:^10} ({:^6}/s)", self.label, d, "---", "---"]
    }
  }
}

impl Info {
  pub fn new(label: &str) -> Self {
    Info {
      label: label.to_string(),
      start: None,
      end: None,
      samples: VecDeque::with_capacity(10),
      count: 0,
      sample_size: 10,
    }
  }
  pub fn add(&mut self, x: usize) {
    self.count += x as u64;
  }
  pub fn tick(&mut self) {
    if let Some(start) = &self.start {
      self.samples.push_front((start.elapsed(),self.count));
      self.samples.truncate(self.sample_size);
    } else {
      self.start = Some(std::time::Instant::now());
    }
  }
}

fn hms(oi: Option<std::time::Duration>) -> String {
  if let Some(i) = oi {
    let t = i.as_secs_f64() as u64;
    let s = t % 60;
    let m = (t / 60) % 60;
    let h = t / 3600;
    format!["{:02}:{:02}:{:02}", h, m, s]
  } else {
    "--:--:--".to_string()
  }
}
