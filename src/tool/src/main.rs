mod export;
mod repl;
mod verify;

use std::path::Path;

use clap::{crate_version, App, AppSettings, Arg, ArgMatches, SubCommand};
use grebedb::{Database, OpenMode, Options};

fn main() -> anyhow::Result<()> {
    let db_path_arg = Arg::with_name("database_path")
        .value_name("DATABASE")
        .help("Path to the directory containing the database.")
        .required(true);

    let zstd_arg = Arg::with_name("zstd").long("zstd");
    let compression_level_arg = Arg::with_name("compression_level")
        .long("compression-level")
        .short("l")
        .help("Compression level where 1 is worst (fastest) and 3 is best (slowest).")
        .default_value("2")
        .possible_values(&["1", "2", "3", "4", "5"]);

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
                .arg(zstd_arg.clone().help("Use Zstandard compression when writing to DESTINATION."))
                .arg(compression_level_arg)
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
                .arg(zstd_arg.clone().help("Use Zstandard decompression when reading from SOURCE."))
        )
        .subcommand(
            SubCommand::with_name("verify")
                .about("Check the database for internal consistency and data integrity.")
                .arg(db_path_arg.clone())
                .arg(
                    Arg::with_name("write")
                        .long("write")
                        .short("w")
                        .help("Open in read & write mode to allow cleanup operations."),
                )
                .arg(
                    Arg::with_name("verbose")
                        .long("verbose")
                        .short("v")
                        .help("Print rough progress."),
                )
        )
        .subcommand(
            SubCommand::with_name("inspect")
                .about("Start a interactive session for browsing and editing the database contents.")
                .arg(db_path_arg.clone())
                .arg(
                    Arg::with_name("write")
                        .long("write")
                        .short("w")
                        .help("Open in read & write mode."),
                )
                .arg(
                    Arg::with_name("batch")
                        .long("batch")
                        .short("b")
                        .help("Enable batch mode for scripting.")
                        .long_help("Enable batch mode for scripting.\n\n\
                            When enabled, any errors are assumed to be undesired and causes the \
                            program to exit. This can be useful for scripts to send commands \
                            using standard input.")
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
            parse_zstd_compression_args(sub_m),
        ),
        ("import", Some(sub_m)) => crate::export::load(
            sub_m.value_of_os("database_path").unwrap().as_ref(),
            sub_m.value_of_os("json_path").unwrap().as_ref(),
            sub_m.is_present("zstd"),
        ),
        ("verify", Some(sub_m)) => crate::verify::verify(
            sub_m.value_of_os("database_path").unwrap().as_ref(),
            sub_m.is_present("write"),
            sub_m.is_present("verbose"),
        ),
        ("inspect", Some(sub_m)) => crate::repl::inspect(
            sub_m.value_of_os("database_path").unwrap().as_ref(),
            sub_m.is_present("write"),
            sub_m.is_present("batch"),
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

fn parse_zstd_compression_args(args: &ArgMatches) -> Option<i32> {
    if args.is_present("zstd") {
        let level = args.value_of("compression_level").unwrap();
        let level: u8 = level.parse().unwrap();

        match level {
            1 => Some(1),
            2 => Some(3),
            3 => Some(9),
            4 => Some(15),
            5 => Some(19),
            _ => unreachable!(),
        }
    } else {
        None
    }
}

fn debug_print_tree_command(database_path: &Path) -> anyhow::Result<()> {
    let mut database = Database::open_path(
        database_path,
        Options {
            open_mode: OpenMode::ReadOnly,
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
