mod export;

use std::path::Path;

use clap::{crate_version, App, AppSettings, Arg, SubCommand};
use grebedb::{Database, DatabaseOpenMode, DatabaseOptions};

fn main() -> anyhow::Result<()> {
    let db_path_arg = Arg::with_name("database_path")
        .value_name("DATABASE")
        .help("Path to the directory containing the database.")
        .required(true);

    let app = App::new("GrebeDB database manipulation tool")
        .version(crate_version!())
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            SubCommand::with_name("export")
                .about("Export the contents of the database to a JSON text sequence (RFC 7464) file.")
                .arg(db_path_arg.clone())
                .arg(
                    Arg::with_name("json_path")
                        .value_name("DESTINATION")
                        .default_value("-")
                        .help("Filename of the exported file."),
                )
        )
        .subcommand(
            SubCommand::with_name("import")
                .about("Import the contents from a JSON text sequence (RFC 7464) file into the database.")
                .arg(db_path_arg.clone())
                .arg(
                    Arg::with_name("json_path")
                        .value_name("SOURCE")
                        .default_value("-")
                        .help("Filename of the source file."),
                )
        )
        .subcommand(
            SubCommand::with_name("debug_print_tree")
                .about("Print the database tree for debugging purposes.")
                .arg(db_path_arg.clone())
        )
        .subcommand(
            SubCommand::with_name("debug_print_page")
                .about("Print a database page for debugging purposes.")
                .arg(
                    Arg::with_name("page_path")
                        .value_name("PATH")
                        .help("Path to the database page.")
                        .required(true)
                )
        );

    let matches = app.get_matches();

    match matches.subcommand() {
        ("export", Some(sub_m)) => crate::export::dump(
            sub_m.value_of_os("database_path").unwrap().as_ref(),
            sub_m.value_of_os("json_path").unwrap().as_ref(),
        ),
        ("import", Some(sub_m)) => crate::export::load(
            sub_m.value_of_os("database_path").unwrap().as_ref(),
            sub_m.value_of_os("json_path").unwrap().as_ref(),
        ),
        ("debug_print_tree", Some(sub_m)) => {
            debug_print_tree_command(sub_m.value_of_os("database_path").unwrap().as_ref())
        }
        ("debug_print_page", Some(sub_m)) => {
            debug_print_page_command(sub_m.value_of_os("page_path").unwrap().as_ref())
        }
        _ => {
            unreachable!();
        }
    }
}

fn debug_print_tree_command(database_path: &Path) -> anyhow::Result<()> {
    let mut database = Database::open_path(
        database_path,
        DatabaseOptions {
            open_mode: DatabaseOpenMode::ReadOnly,
            ..Default::default()
        },
    )?;
    database.debug_print_tree()?;

    Ok(())
}

fn debug_print_page_command(path: &Path) -> anyhow::Result<()> {
    grebedb::debug_print_page(path)?;

    Ok(())
}
