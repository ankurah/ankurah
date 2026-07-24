mod common;

use common::{load_blog_config, load_minimal_config};

use ankurah_jwt_auth::PolicyConfig;

#[test]
fn test_load_blog_config() {
    let config = load_blog_config();
    assert!(config.roles.contains_key("Admin"));
    assert!(config.roles.contains_key("Editor"));
    assert!(config.roles.contains_key("Author"));
    assert!(config.roles.contains_key("Reader"));
    assert_eq!(config.roles.len(), 4);

    // Admin has wildcard
    assert_eq!(config.roles["Admin"], vec!["*"]);

    // Editor has expected privileges
    let editor_privs = &config.roles["Editor"];
    assert!(editor_privs.contains(&"view_posts".to_string()));
    assert!(editor_privs.contains(&"manage_posts".to_string()));

    // Collections defined
    assert!(config.collections.contains_key("post"));
    assert!(config.collections.contains_key("comment"));
    assert!(config.collections.contains_key("user"));
    assert!(config.collections.contains_key("tag"));
}

#[test]
fn test_load_minimal_config() {
    let config = load_minimal_config();
    assert_eq!(config.roles.len(), 1);
    assert!(config.roles.contains_key("Admin"));
    assert_eq!(config.roles["Admin"], vec!["*"]);
    assert!(config.collections.is_empty());
}

#[test]
fn test_editor_can_access_post_collection() {
    let config = load_blog_config();
    assert!(config.can_access_collection(&[String::from("Editor")], "post"));
}

#[test]
fn test_editor_can_write_post_collection() {
    let config = load_blog_config();
    assert!(config.can_write_collection(&[String::from("Editor")], "post"));
}

#[test]
fn test_reader_cannot_access_post_collection() {
    let config = load_blog_config();
    assert!(!config.can_access_collection(&[String::from("Reader")], "post"));
}

#[test]
fn test_reader_can_access_comment_collection() {
    let config = load_blog_config();
    assert!(config.can_access_collection(&[String::from("Reader")], "comment"));
}

#[test]
fn test_reader_cannot_write_comment_collection() {
    let config = load_blog_config();
    assert!(!config.can_write_collection(&[String::from("Reader")], "comment"));
}

#[test]
fn test_author_can_read_but_not_write_post() {
    let config = load_blog_config();
    assert!(config.can_access_collection(&[String::from("Author")], "post"));
    assert!(!config.can_write_collection(&[String::from("Author")], "post"));
}

#[test]
fn test_unknown_role_denied() {
    let config = load_blog_config();
    assert!(!config.can_access_collection(&[String::from("Hacker")], "post"));
    assert!(!config.can_write_collection(&[String::from("Hacker")], "post"));
}

#[test]
fn test_unknown_collection_denied() {
    let config = load_blog_config();
    assert!(!config.can_access_collection(&[String::from("Editor")], "secret_stuff"));
    assert!(!config.can_write_collection(&[String::from("Editor")], "secret_stuff"));
}

#[test]
fn test_wildcard_privilege_grants_all_access() {
    let config = load_blog_config();
    let admin = [String::from("Admin")];

    assert!(config.can_access_collection(&admin, "post"));
    assert!(config.can_write_collection(&admin, "post"));
    assert!(config.can_access_collection(&admin, "comment"));
    assert!(config.can_write_collection(&admin, "comment"));
    assert!(config.can_access_collection(&admin, "user"));
    assert!(config.can_write_collection(&admin, "user"));
    assert!(config.can_access_collection(&admin, "tag"));
    assert!(config.can_write_collection(&admin, "tag"));

    // Wildcard also grants access to collections not explicitly defined
    assert!(config.can_access_collection(&admin, "anything_else"));
    assert!(config.can_write_collection(&admin, "anything_else"));
}

#[test]
fn test_minimal_config_wildcard_admin() {
    let config = load_minimal_config();
    assert!(config.can_access_collection(&[String::from("Admin")], "anything"));
}

#[test]
fn test_default_config_is_empty() {
    let config = PolicyConfig::default();
    assert!(config.roles.is_empty());
    assert!(config.collections.is_empty());
}

#[test]
fn test_multi_role_access() {
    let config = load_blog_config();
    let roles = vec![String::from("Reader"), String::from("Editor")];
    assert!(config.can_access_collection(&roles, "post"));
    assert!(config.can_write_collection(&roles, "post"));
    assert!(config.can_access_collection(&roles, "comment"));
}
