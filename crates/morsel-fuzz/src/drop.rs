use std::collections::HashSet;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DropMask {
    pub drop_join: bool,
    pub drop_filter: bool,
    pub drop_conjuncts: HashSet<usize>,
    pub drop_project: bool,
    pub drop_agg: bool,
    pub drop_limit: bool,
}

impl DropMask {
    pub fn parse(s: &str) -> Self {
        let mut mask = DropMask::default();
        for item in s.split(',') {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }
            if item == "join" {
                mask.drop_join = true;
            } else if item == "filter" {
                mask.drop_filter = true;
            } else if item == "project" {
                mask.drop_project = true;
            } else if item == "agg" {
                mask.drop_agg = true;
            } else if item == "limit" {
                mask.drop_limit = true;
            } else if let Some(rest) = item.strip_prefix("conjunct:") {
                if let Ok(idx) = rest.parse::<usize>() {
                    mask.drop_conjuncts.insert(idx);
                }
            }
        }
        mask
    }
}

impl std::fmt::Display for DropMask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if self.drop_join {
            parts.push("join".to_string());
        }
        if self.drop_filter {
            parts.push("filter".to_string());
        }
        let mut sorted_conjuncts: Vec<_> = self.drop_conjuncts.iter().copied().collect();
        sorted_conjuncts.sort_unstable();
        for idx in sorted_conjuncts {
            parts.push(format!("conjunct:{}", idx));
        }
        if self.drop_project {
            parts.push("project".to_string());
        }
        if self.drop_agg {
            parts.push("agg".to_string());
        }
        if self.drop_limit {
            parts.push("limit".to_string());
        }
        write!(f, "{}", parts.join(","))
    }
}
