use crate::Error;
use hashbrown::HashMap;
use std::collections::VecDeque;
use digit_group::FormatGroup;

pub struct Progress {
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

impl Progress {
  pub fn new(stages: &[&str]) -> Self {
    let mut info = HashMap::new();
    for s in stages.iter() {
      info.insert(s.to_string(), Info::new(s));
    }
    Self {
      stages: stages.iter().map(|s| s.to_string()).collect(),
      info,
    }
  }
  pub fn add(&mut self, label: &str, x: usize) {
    if let Some(info) = self.info.get_mut(label) {
      info.add(x);
    }
  }
  pub fn push_err(&mut self, label: &str, err: &Error) {
    if let Some(info) = self.info.get_mut(label) {
      info.push_err(err);
    }
  }
  pub fn start(&mut self, label: &str) {
    if let Some(info) = self.info.get_mut(label) {
      info.start();
    } else {
      panic!["bad start label {}", label];
    }
  }
  pub fn end(&mut self, label: &str) {
    if let Some(info) = self.info.get_mut(label) {
      info.end();
    } else {
      panic!["bad end label {}", label];
    }
  }
  pub fn tick(&mut self) {
    for (_,info) in self.info.iter_mut() {
      info.tick();
    }
  }
}

pub struct Info {
  label: String,
  start: Option<std::time::Instant>,
  end: Option<std::time::Instant>,
  samples: VecDeque<(std::time::Duration,u64)>,
  count: u64,
  sample_size: usize,
  errors: VecDeque<String>,
  error_size: usize,
}

impl std::fmt::Display for Info {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let d = hms(match (self.start,self.end) {
      (Some(s),Some(e)) => e.checked_duration_since(s),
      (Some(s),None) => Some(s.elapsed()),
      _ => None
    });
    if let (Some(s),Some(e)) = (self.start,self.end) {
      let rate = (self.count as f64) / e.duration_since(s).as_secs_f64();
      write![f, "[{:<9} {}] {:>13} ({:>9}/s)", self.label, d,
        un(self.count), un(rate.round() as u64)]
    } else if let (Some(first),Some(last)) = (self.samples.front(),self.samples.back()) {
      let e = first.0.as_secs_f64() - last.0.as_secs_f64();
      if e < 0.001 && self.start.is_some() {
        let rate = (self.count as f64) / self.start.unwrap().elapsed().as_secs_f64();
        write![f, "[{:<9} {}] {:>13} ({:>9}/s)", self.label, d,
          un(self.count), un(rate.round() as u64)]
      } else if e < 0.001 {
        write![f, "[{:<9} {}] {:^13} ({:^9}/s)", self.label, d,
          un(self.count), "---"]
      } else {
        let rate = ((first.1 - last.1) as f64) / e;
        write![f, "[{:<9} {}] {:>13} ({:>9}/s)", self.label, d,
          un(self.count), un(rate.round() as u64)]
      }
    } else {
      write![f, "[{:<9} {}] {:^13} ({:^9}/s)", self.label, d, "---", "---"]
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
      errors: VecDeque::with_capacity(10),
      count: 0,
      sample_size: 20,
      error_size: 10,
    }
  }
  pub fn add(&mut self, x: usize) {
    self.count += x as u64;
  }
  pub fn push_err(&mut self, err: &Error) {
    self.errors.push_front(format!["{}", err]);
    self.errors.truncate(self.error_size);
  }
  pub fn start(&mut self) {
    self.start = Some(std::time::Instant::now());
  }
  pub fn end(&mut self) {
    self.end = Some(std::time::Instant::now());
  }
  pub fn tick(&mut self) {
    if self.end.is_some() { return }
    if let Some(start) = &self.start {
      self.samples.push_front((start.elapsed(),self.count));
      self.samples.truncate(self.sample_size);
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

fn un<T: FormatGroup>(n: T) -> String {
  n.format_custom('.','_',3,3,false)
}
