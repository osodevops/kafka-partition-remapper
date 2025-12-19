//! Maps X.509 Distinguished Names to principal names.
//!
//! This module provides Kafka-compatible SSL principal mapping, following
//! Apache Kafka's `SslPrincipalMapper` design with rule-based transformation
//! of certificate Subject DNs.
//!
//! # Rule Syntax
//!
//! Rules are comma-separated and processed in order until one matches:
//!
//! - `DEFAULT` - Return the DN unchanged
//! - `RULE:pattern/replacement/[LU]` - Regex replacement with optional case transform
//!
//! # Case Transformation
//!
//! - `L` or `l` - Lowercase the result
//! - `U` or `u` - Uppercase the result
//! - Empty or omitted - Keep original case
//!
//! # Examples
//!
//! ```
//! use kafka_remapper_core::auth::SslPrincipalMapper;
//!
//! // Extract CN and lowercase it
//! let mapper = SslPrincipalMapper::from_rules("RULE:^CN=([^,]+),.*$/$1/L,DEFAULT").unwrap();
//! let dn = "CN=Kafka-Client,OU=Engineering,O=Company,C=US";
//! assert_eq!(mapper.get_name(dn).unwrap(), "kafka-client");
//!
//! // Default rule returns full DN
//! let mapper = SslPrincipalMapper::default();
//! assert_eq!(mapper.get_name(dn).unwrap(), dn);
//! ```
//!
//! # Kafka Compatibility
//!
//! This implementation is compatible with Apache Kafka's `ssl.principal.mapping.rules`
//! configuration. The same rule syntax can be used in both systems.

use regex::Regex;
use std::fmt;

/// Rule for mapping DN to principal name.
#[derive(Debug, Clone)]
pub enum MappingRule {
    /// Return the DN unchanged.
    Default,
    /// Apply regex pattern and replacement.
    Pattern {
        /// Compiled regex pattern.
        pattern: Regex,
        /// Replacement string (supports backreferences like `$1`, `$2`).
        replacement: String,
        /// Optional case transformation.
        case_transform: Option<CaseTransform>,
    },
}

/// Case transformation to apply after replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseTransform {
    /// Convert to lowercase.
    Lower,
    /// Convert to uppercase.
    Upper,
}

/// Maps X.509 Distinguished Names to principal names.
///
/// # Rule Processing
///
/// Rules are processed in order. The first rule that matches the DN is applied,
/// and its result is returned. If no pattern rules match, the `DEFAULT` rule
/// (if present) returns the DN unchanged.
///
/// # Thread Safety
///
/// `SslPrincipalMapper` is `Send + Sync` and can be shared across threads.
#[derive(Debug, Clone)]
pub struct SslPrincipalMapper {
    rules: Vec<MappingRule>,
}

impl SslPrincipalMapper {
    /// Create a mapper with default rules (return DN unchanged).
    pub fn default() -> Self {
        Self {
            rules: vec![MappingRule::Default],
        }
    }

    /// Parse rules from a comma-separated string.
    ///
    /// # Rule Format
    ///
    /// - `DEFAULT` - Use DN as-is
    /// - `RULE:pattern/replacement/` - Regex replacement, keep case
    /// - `RULE:pattern/replacement/L` - Regex replacement, lowercase result
    /// - `RULE:pattern/replacement/U` - Regex replacement, uppercase result
    ///
    /// # Examples
    ///
    /// ```
    /// use kafka_remapper_core::auth::SslPrincipalMapper;
    ///
    /// // Single rule: extract CN
    /// let mapper = SslPrincipalMapper::from_rules("RULE:^CN=([^,]+).*$/$1/").unwrap();
    ///
    /// // Multiple rules with fallback
    /// let mapper = SslPrincipalMapper::from_rules(
    ///     "RULE:^CN=([^,]+),OU=Admin.*$/$1/U,RULE:^CN=([^,]+).*$/$1/L,DEFAULT"
    /// ).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A rule doesn't start with `RULE:` or equal `DEFAULT`
    /// - A regex pattern is invalid
    /// - The case transform flag is not `L`, `U`, or empty
    pub fn from_rules(rules_str: &str) -> Result<Self, MapperError> {
        let mut rules = Vec::new();

        // Split rules on commas, but not commas inside square brackets (regex character classes)
        for rule_str in Self::split_rules(rules_str) {
            let rule_str = rule_str.trim();
            if rule_str.is_empty() {
                continue;
            }
            let rule = Self::parse_rule(rule_str)?;
            rules.push(rule);
        }

        if rules.is_empty() {
            rules.push(MappingRule::Default);
        }

        Ok(Self { rules })
    }

    /// Split rules string on commas that separate rules.
    ///
    /// Rules are separated by commas, but commas can also appear inside regex
    /// patterns. We split only when a comma is followed by `RULE:` or `DEFAULT`
    /// (case-insensitive), which indicates the start of a new rule.
    fn split_rules(rules_str: &str) -> Vec<&str> {
        let mut rules = Vec::new();
        let mut start = 0;
        let chars: Vec<char> = rules_str.chars().collect();
        let len = chars.len();
        let mut i = 0;

        while i < len {
            if chars[i] == ',' {
                // Check if followed by whitespace then RULE: or DEFAULT
                let mut j = i + 1;

                // Skip whitespace
                while j < len && chars[j].is_whitespace() {
                    j += 1;
                }

                // Check for RULE: or DEFAULT
                let remaining: String = chars[j..].iter().collect();
                let is_rule_start = remaining.to_uppercase().starts_with("RULE:")
                    || remaining.to_uppercase().starts_with("DEFAULT");

                if is_rule_start {
                    // This comma separates rules
                    let byte_pos = rules_str
                        .char_indices()
                        .nth(i)
                        .map(|(pos, _)| pos)
                        .unwrap_or(rules_str.len());
                    rules.push(&rules_str[start..byte_pos]);

                    // Find byte position after comma
                    let next_byte_pos = rules_str
                        .char_indices()
                        .nth(i + 1)
                        .map(|(pos, _)| pos)
                        .unwrap_or(rules_str.len());
                    start = next_byte_pos;
                }
            }
            i += 1;
        }

        // Add the last segment
        if start < rules_str.len() {
            rules.push(&rules_str[start..]);
        }

        rules
    }

    fn parse_rule(rule_str: &str) -> Result<MappingRule, MapperError> {
        if rule_str.eq_ignore_ascii_case("DEFAULT") {
            return Ok(MappingRule::Default);
        }

        if !rule_str.starts_with("RULE:") {
            return Err(MapperError::InvalidRule(format!(
                "Rule must start with 'RULE:' or be 'DEFAULT': {}",
                rule_str
            )));
        }

        let rule_body = &rule_str[5..];

        // Parse: pattern/replacement/[LU]
        // Split into at most 3 parts
        let parts: Vec<&str> = rule_body.splitn(3, '/').collect();

        if parts.len() < 2 {
            return Err(MapperError::InvalidRule(format!(
                "Rule must have pattern/replacement: {}",
                rule_str
            )));
        }

        let pattern_str = parts[0];
        let replacement = parts[1].to_string();

        let case_transform = if parts.len() > 2 {
            match parts[2] {
                "L" | "l" => Some(CaseTransform::Lower),
                "U" | "u" => Some(CaseTransform::Upper),
                "" => None,
                other => {
                    return Err(MapperError::InvalidRule(format!(
                        "Invalid case transform '{}', expected L or U",
                        other
                    )))
                }
            }
        } else {
            None
        };

        let pattern = Regex::new(pattern_str)
            .map_err(|e| MapperError::InvalidPattern(format!("{}: {}", pattern_str, e)))?;

        Ok(MappingRule::Pattern {
            pattern,
            replacement,
            case_transform,
        })
    }

    /// Map a Distinguished Name to a principal name.
    ///
    /// Processes rules in order until one matches. For pattern rules, the
    /// regex must match for the rule to apply. The `DEFAULT` rule always
    /// matches and returns the DN unchanged.
    ///
    /// # Backreferences
    ///
    /// Replacement strings support backreferences:
    /// - `$0` - The entire match
    /// - `$1`, `$2`, etc. - Capture groups
    ///
    /// # Examples
    ///
    /// ```
    /// use kafka_remapper_core::auth::SslPrincipalMapper;
    ///
    /// let mapper = SslPrincipalMapper::from_rules("RULE:^CN=([^,]+).*$/$1/L").unwrap();
    /// let result = mapper.get_name("CN=KafkaUser,O=Company").unwrap();
    /// assert_eq!(result, "kafkauser");
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `NoMatchingRule` if no rules match the DN. This typically
    /// happens when there's no `DEFAULT` rule and no pattern rules match.
    pub fn get_name(&self, dn: &str) -> Result<String, MapperError> {
        for rule in &self.rules {
            match rule {
                MappingRule::Default => return Ok(dn.to_string()),
                MappingRule::Pattern {
                    pattern,
                    replacement,
                    case_transform,
                } => {
                    if let Some(captures) = pattern.captures(dn) {
                        let mut result = replacement.clone();

                        // Handle backreferences ($0, $1, $2, etc.)
                        // Process in reverse order to handle $10 before $1
                        for i in (0..captures.len()).rev() {
                            if let Some(m) = captures.get(i) {
                                let placeholder = format!("${}", i);
                                result = result.replace(&placeholder, m.as_str());
                            }
                        }

                        // Handle named groups
                        for name in pattern.capture_names().flatten() {
                            if let Some(m) = captures.name(name) {
                                let placeholder = format!("${{{}}}", name);
                                result = result.replace(&placeholder, m.as_str());
                            }
                        }

                        // Apply case transformation
                        let result = match case_transform {
                            Some(CaseTransform::Lower) => result.to_lowercase(),
                            Some(CaseTransform::Upper) => result.to_uppercase(),
                            None => result,
                        };

                        return Ok(result);
                    }
                }
            }
        }

        Err(MapperError::NoMatchingRule(dn.to_string()))
    }

    /// Get the number of rules in this mapper.
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }
}

/// Errors from principal mapping.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MapperError {
    /// The rule syntax is invalid.
    #[error("invalid mapping rule: {0}")]
    InvalidRule(String),
    /// The regex pattern is invalid.
    #[error("invalid regex pattern: {0}")]
    InvalidPattern(String),
    /// No rule matched the Distinguished Name.
    #[error("no mapping rule matched DN: {0}")]
    NoMatchingRule(String),
}

impl fmt::Display for SslPrincipalMapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SslPrincipalMapper({} rules)", self.rules.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_rule() {
        let mapper = SslPrincipalMapper::from_rules("DEFAULT").unwrap();
        let dn = "CN=client1,OU=Engineering,O=Company,C=US";
        assert_eq!(mapper.get_name(dn).unwrap(), dn);
    }

    #[test]
    fn test_default_mapper() {
        let mapper = SslPrincipalMapper::default();
        let dn = "CN=client1,OU=Engineering,O=Company,C=US";
        assert_eq!(mapper.get_name(dn).unwrap(), dn);
    }

    #[test]
    fn test_extract_cn() {
        let mapper = SslPrincipalMapper::from_rules("RULE:^CN=([^,]+),.*$/$1/").unwrap();
        let dn = "CN=kafka-client,OU=Engineering,O=Company,C=US";
        assert_eq!(mapper.get_name(dn).unwrap(), "kafka-client");
    }

    #[test]
    fn test_extract_cn_no_trailing_slash() {
        // Without trailing slash, no case transform
        let mapper = SslPrincipalMapper::from_rules("RULE:^CN=([^,]+).*$/$1").unwrap();
        let dn = "CN=kafka-client,OU=Engineering";
        assert_eq!(mapper.get_name(dn).unwrap(), "kafka-client");
    }

    #[test]
    fn test_extract_cn_lowercase() {
        let mapper = SslPrincipalMapper::from_rules("RULE:^CN=([^,]+),.*$/$1/L").unwrap();
        let dn = "CN=Kafka-Client,OU=Engineering,O=Company,C=US";
        assert_eq!(mapper.get_name(dn).unwrap(), "kafka-client");
    }

    #[test]
    fn test_extract_cn_uppercase() {
        let mapper = SslPrincipalMapper::from_rules("RULE:^CN=([^,]+),.*$/$1/U").unwrap();
        let dn = "CN=Kafka-Client,OU=Engineering,O=Company,C=US";
        assert_eq!(mapper.get_name(dn).unwrap(), "KAFKA-CLIENT");
    }

    #[test]
    fn test_fallback_to_default() {
        let mapper = SslPrincipalMapper::from_rules("RULE:^OU=([^,]+).*$/$1/,DEFAULT").unwrap();
        // DN doesn't start with OU, so pattern won't match
        let dn = "CN=client1,O=Company";
        // Falls through to DEFAULT rule
        assert_eq!(mapper.get_name(dn).unwrap(), dn);
    }

    #[test]
    fn test_multiple_rules_first_match() {
        let mapper = SslPrincipalMapper::from_rules(
            "RULE:^CN=admin-([^,]+).*$/admin:$1/U,RULE:^CN=([^,]+).*$/$1/L,DEFAULT",
        )
        .unwrap();

        // Admin user matches first rule
        let admin_dn = "CN=admin-SuperUser,OU=Admins";
        assert_eq!(mapper.get_name(admin_dn).unwrap(), "ADMIN:SUPERUSER");

        // Regular user matches second rule
        let user_dn = "CN=RegularUser,OU=Users";
        assert_eq!(mapper.get_name(user_dn).unwrap(), "regularuser");

        // Weird DN matches default rule
        let weird_dn = "OU=WeirdFormat";
        assert_eq!(mapper.get_name(weird_dn).unwrap(), weird_dn);
    }

    #[test]
    fn test_no_matching_rule_error() {
        let mapper = SslPrincipalMapper::from_rules("RULE:^OU=([^,]+).*$/$1/").unwrap();
        let dn = "CN=client1,O=Company";
        let result = mapper.get_name(dn);
        assert!(matches!(result, Err(MapperError::NoMatchingRule(_))));
    }

    #[test]
    fn test_invalid_rule_syntax() {
        let result = SslPrincipalMapper::from_rules("INVALID:rule");
        assert!(matches!(result, Err(MapperError::InvalidRule(_))));
    }

    #[test]
    fn test_invalid_regex_pattern() {
        let result = SslPrincipalMapper::from_rules("RULE:[invalid/replacement/");
        assert!(matches!(result, Err(MapperError::InvalidPattern(_))));
    }

    #[test]
    fn test_invalid_case_transform() {
        let result = SslPrincipalMapper::from_rules("RULE:^CN=(.*)$/$1/X");
        assert!(matches!(result, Err(MapperError::InvalidRule(_))));
    }

    #[test]
    fn test_empty_rules_string() {
        // Empty string should result in default rule
        let mapper = SslPrincipalMapper::from_rules("").unwrap();
        assert_eq!(mapper.rule_count(), 1);
        let dn = "CN=test";
        assert_eq!(mapper.get_name(dn).unwrap(), dn);
    }

    #[test]
    fn test_whitespace_handling() {
        let mapper =
            SslPrincipalMapper::from_rules("  RULE:^CN=([^,]+).*$/$1/  ,  DEFAULT  ").unwrap();
        assert_eq!(mapper.rule_count(), 2);
    }

    #[test]
    fn test_case_insensitive_default() {
        let mapper = SslPrincipalMapper::from_rules("default").unwrap();
        let dn = "CN=test";
        assert_eq!(mapper.get_name(dn).unwrap(), dn);
    }

    #[test]
    fn test_case_insensitive_transform_flag() {
        let mapper_lower = SslPrincipalMapper::from_rules("RULE:^CN=(.*)$/$1/l").unwrap();
        let mapper_upper = SslPrincipalMapper::from_rules("RULE:^CN=(.*)$/$1/u").unwrap();

        let dn = "CN=TestUser";
        assert_eq!(mapper_lower.get_name(dn).unwrap(), "testuser");
        assert_eq!(mapper_upper.get_name(dn).unwrap(), "TESTUSER");
    }

    #[test]
    fn test_multiple_capture_groups() {
        let mapper =
            SslPrincipalMapper::from_rules("RULE:^CN=([^,]+),OU=([^,]+).*$/$2:$1/").unwrap();
        let dn = "CN=alice,OU=Engineering,O=Company";
        assert_eq!(mapper.get_name(dn).unwrap(), "Engineering:alice");
    }

    #[test]
    fn test_full_match_backreference() {
        let mapper = SslPrincipalMapper::from_rules("RULE:^(CN=[^,]+).*$/[$0]/").unwrap();
        let dn = "CN=alice,OU=Eng";
        assert_eq!(mapper.get_name(dn).unwrap(), "[CN=alice,OU=Eng]");
    }

    #[test]
    fn test_display() {
        let mapper = SslPrincipalMapper::from_rules("RULE:^CN=(.*)$/$1/,DEFAULT").unwrap();
        assert_eq!(format!("{}", mapper), "SslPrincipalMapper(2 rules)");
    }

    #[test]
    fn test_real_world_kafka_rules() {
        // Common Kafka rule: extract CN and lowercase
        let mapper =
            SslPrincipalMapper::from_rules("RULE:^CN=([a-zA-Z0-9._-]+),.*$/$1/L,DEFAULT").unwrap();

        // Service account with standard naming
        let service_dn = "CN=kafka-producer-service,OU=Services,O=MyCompany,C=US";
        assert_eq!(
            mapper.get_name(service_dn).unwrap(),
            "kafka-producer-service"
        );

        // User certificate
        let user_dn = "CN=Alice.Smith,OU=Users,O=MyCompany,C=US";
        assert_eq!(mapper.get_name(user_dn).unwrap(), "alice.smith");

        // Weird format falls back to default
        let weird_dn = "OU=Special,O=Org";
        assert_eq!(mapper.get_name(weird_dn).unwrap(), weird_dn);
    }
}
