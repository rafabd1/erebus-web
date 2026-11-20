use globset::{GlobBuilder, GlobMatcher};
use ipnet::IpNet;
use regex::Regex;
use std::net::IpAddr;
use url::Url;

use crate::model::{ScopeAction, ScopeDecision, ScopeMatcher, ScopeRuleInput, ScopeState};

#[derive(Debug)]
pub struct ScopeProfile {
    rules: Vec<CompiledRule>,
    has_includes: bool,
}

#[derive(Debug)]
struct CompiledRule {
    id: String,
    action: ScopeAction,
    matcher: CompiledMatcher,
}

#[derive(Debug)]
enum CompiledMatcher {
    Host(String),
    HostWildcard(GlobMatcher),
    UrlWildcard(GlobMatcher),
    Regex(Regex),
    Cidr(IpNet),
}

impl ScopeProfile {
    pub fn compile(
        rules: impl IntoIterator<Item = (String, ScopeRuleInput)>,
    ) -> anyhow::Result<Self> {
        let mut compiled = Vec::new();
        let mut has_includes = false;
        for (id, rule) in rules {
            if !rule.enabled {
                continue;
            }
            has_includes |= rule.action == ScopeAction::Include;
            let matcher = match rule.matcher {
                ScopeMatcher::Host => CompiledMatcher::Host(rule.pattern.to_ascii_lowercase()),
                ScopeMatcher::HostWildcard => CompiledMatcher::HostWildcard(
                    GlobBuilder::new(&rule.pattern.to_ascii_lowercase())
                        .literal_separator(true)
                        .build()?
                        .compile_matcher(),
                ),
                ScopeMatcher::UrlWildcard => CompiledMatcher::UrlWildcard(
                    GlobBuilder::new(&rule.pattern)
                        .literal_separator(false)
                        .build()?
                        .compile_matcher(),
                ),
                ScopeMatcher::Regex => CompiledMatcher::Regex(Regex::new(&rule.pattern)?),
                ScopeMatcher::Cidr => CompiledMatcher::Cidr(rule.pattern.parse()?),
            };
            compiled.push(CompiledRule {
                id,
                action: rule.action,
                matcher,
            });
        }
        Ok(Self {
            rules: compiled,
            has_includes,
        })
    }

    pub fn evaluate(&self, url: &Url) -> ScopeDecision {
        let mut first_include = None;
        for rule in &self.rules {
            if !rule.matches(url) {
                continue;
            }
            if rule.action == ScopeAction::Exclude {
                return ScopeDecision {
                    state: ScopeState::OutOfScope,
                    rule_id: Some(rule.id.clone()),
                    reason: "matched exclude rule".into(),
                };
            }
            if first_include.is_none() {
                first_include = Some(rule.id.clone());
            }
        }
        if let Some(rule_id) = first_include {
            return ScopeDecision {
                state: ScopeState::InScope,
                rule_id: Some(rule_id),
                reason: "matched include rule".into(),
            };
        }
        if self.has_includes {
            ScopeDecision {
                state: ScopeState::OutOfScope,
                rule_id: None,
                reason: "did not match any include rule".into(),
            }
        } else {
            ScopeDecision {
                state: ScopeState::InScope,
                rule_id: None,
                reason: "no include rules restrict this profile".into(),
            }
        }
    }
}

impl CompiledRule {
    fn matches(&self, url: &Url) -> bool {
        let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
        match &self.matcher {
            CompiledMatcher::Host(expected) => host == *expected,
            CompiledMatcher::HostWildcard(matcher) => matcher.is_match(&host),
            CompiledMatcher::UrlWildcard(matcher) => matcher.is_match(url.as_str()),
            CompiledMatcher::Regex(regex) => regex.is_match(url.as_str()),
            CompiledMatcher::Cidr(network) => {
                host.parse::<IpAddr>().is_ok_and(|ip| network.contains(&ip))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(action: ScopeAction, matcher: ScopeMatcher, pattern: &str) -> ScopeRuleInput {
        ScopeRuleInput {
            action,
            matcher,
            pattern: pattern.into(),
            enabled: true,
        }
    }

    #[test]
    fn excludes_win_over_includes() {
        let profile = ScopeProfile::compile([
            (
                "include".into(),
                rule(
                    ScopeAction::Include,
                    ScopeMatcher::HostWildcard,
                    "*.example.com",
                ),
            ),
            (
                "exclude".into(),
                rule(
                    ScopeAction::Exclude,
                    ScopeMatcher::Host,
                    "admin.example.com",
                ),
            ),
        ])
        .unwrap();
        assert_eq!(
            profile
                .evaluate(&Url::parse("https://api.example.com/a").unwrap())
                .state,
            ScopeState::InScope
        );
        assert_eq!(
            profile
                .evaluate(&Url::parse("https://admin.example.com/a").unwrap())
                .state,
            ScopeState::OutOfScope
        );
    }

    #[test]
    fn profile_without_includes_defaults_to_in_scope() {
        let profile = ScopeProfile::compile([(
            "exclude".into(),
            rule(ScopeAction::Exclude, ScopeMatcher::Host, "blocked.example"),
        )])
        .unwrap();
        assert_eq!(
            profile
                .evaluate(&Url::parse("https://other.example/").unwrap())
                .state,
            ScopeState::InScope
        );
    }

    #[test]
    fn url_wildcards_cross_path_segments() {
        let profile = ScopeProfile::compile([(
            "include".into(),
            rule(
                ScopeAction::Include,
                ScopeMatcher::UrlWildcard,
                "https://example.com/api/*",
            ),
        )])
        .unwrap();
        assert_eq!(
            profile
                .evaluate(&Url::parse("https://example.com/api/v1/users/1").unwrap())
                .state,
            ScopeState::InScope
        );
    }
}
