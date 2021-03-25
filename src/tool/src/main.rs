mod dump;

use clap::{crate_version, App, Arg, SubCommand};

fn main() -> anyhow::Result<()> {
    let matches = App::new("GrebeDB database manipulation tool")
        .version(crate_version!())
        .arg(
            Arg::with_name("database_path")
                .value_name("DATABASE")
                .help("Path to the directory containing the database.")
                .required(true),
        )
        .subcommand(
            SubCommand::with_name("export")
                .about("Export the contents of the database to a JSON text sequence (RFC 7464) file.")
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
                .arg(
                    Arg::with_name("json_path")
                        .value_name("SOURCE")
                        .default_value("-")
                        .help("Filename of the source file."),
                )
        )
        .get_matches();

    match matches.subcommand() {
        ("export", Some(sub_m)) => crate::dump::dump(
            matches.value_of_os("database_path").unwrap().as_ref(),
            sub_m.value_of_os("json_path").unwrap().as_ref(),
        ),
        ("import", Some(sub_m)) => crate::dump::load(
            matches.value_of_os("database_path").unwrap().as_ref(),
            sub_m.value_of_os("json_path").unwrap().as_ref(),
        ),
        _ => Ok(()),
    }
}
