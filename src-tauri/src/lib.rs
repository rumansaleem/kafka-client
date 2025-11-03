// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
mod core;
mod kafka;

use crate::core::{commands, config::ApplicationState};

#[cfg_attr(mobile, tauri::mobile_enrty_point)]
pub fn run() {
    let ctx = tauri::generate_context!();
    let mut builder = tauri::Builder::default().plugin(tauri_plugin_os::init());

    #[cfg(debug_assertions)]
    {
        builder = builder.plugin(tauri_plugin_devtools::init());
    }

    builder
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_store::Builder::new().build())
        .manage(ApplicationState::load())
        .invoke_handler(tauri::generate_handler![
            commands::get_current_cluster,
            commands::get_topics,
            commands::fetch_topic_configs,
            commands::alter_topic_configs,
            commands::get_all_active_consumers,
            commands::consume_topic_by_timestamp,
            commands::stop_consumer,
            commands::create_topic,
            commands::delete_topic,
            commands::get_groups,
            commands::get_group_offsets,
            commands::create_group_offsets,
            commands::delete_consumer_group,
        ])
        .run(ctx)
        .expect("error while running tauri application");
}
