mod encoding;

use std::convert::TryInto;
use std::path::Path;

use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use grebedb::{Database, OpenMode, Options};
use rustyline::{error::ReadlineError, Editor};

use self::encoding::{DocumentFormat, Encoding};

pub fn inspect(database_path: &Path, write: bool, batch_mode: bool) -> anyhow::Result<()> {
    let options = Options {
        open_mode: if write {
            OpenMode::LoadOnly
        } else {
            OpenMode::ReadOnly
        },
        automatic_flush: false,
        ..Default::default()
    };

    let mut database = Database::open_path(database_path, options)?;

    let mut readline = Editor::<()>::new();

    if !batch_mode {
        eprintln!("Welcome to the inspector. Type `help` and press enter for list of commands.");
    }

    loop {
        let line = readline.readline(">> ");

        match line {
            Ok(line) => {
                readline.add_history_entry(line.as_str());

                match execute_command(&mut database, &line) {
                    Ok(command_result) => match command_result {
                        CommandResult::Continue => {}
                        CommandResult::Exit => {
                            break;
                        }
                        CommandResult::Error(error) => {
                            if batch_mode {
                                return Err(error);
                            } else {
                                eprintln!("{}", error);
                            }
                        }
                    },
                    Err(error) => {
                        if batch_mode {
                            return Err(error);
                        } else {
                            eprintln!("Error: {}", error);
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
            Err(error) => return Err(anyhow::Error::new(error)),
        }
    }

    eprintln!("Exiting.");

    Ok(())
}

fn build_command_args() -> App<'static, 'static> {
    let key_format_arg = Arg::with_name("key_encoding")
        .value_name("ENCODING")
        .long("key-encoding")
        .short("K")
        .help("Use the given encoding to show keys in textual form.")
        .possible_values(&Encoding::list())
        .default_value(Encoding::Utf8.into());

    let value_format_arg = Arg::with_name("value_encoding")
        .value_name("ENCODING")
        .long("value-encoding")
        .short("V")
        .help("Use the given encoding to show values in textual form.")
        .possible_values(&Encoding::list())
        .default_value(Encoding::Utf8.into());

    App::new("")
        .setting(AppSettings::DisableVersion)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::NoBinaryName)
        .subcommand(
            SubCommand::with_name("count").about("Get number of key-value pairs in the database."),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get key-value pair by its key.")
                .arg(Arg::with_name("key").required(true))
                .arg(key_format_arg.clone())
                .arg(value_format_arg.clone()),
        )
        .subcommand(
            SubCommand::with_name("scan")
                .about("Get all key-value pairs within a range.")
                .arg(
                    Arg::with_name("key_start")
                        .value_name("START")
                        .help("Starting key range (inclusive)."),
                )
                .arg(
                    Arg::with_name("key_end")
                        .value_name("END")
                        .help("Ending key range (exclusive)."),
                )
                .arg(
                    Arg::with_name("keys_only")
                        .long("keys-only")
                        .short("k")
                        .help("Show only keys and don't print values."),
                )
                .arg(key_format_arg.clone())
                .arg(value_format_arg.clone()),
        )
        .subcommand(
            SubCommand::with_name("put")
                .about("Insert a key-value pair.")
                .arg(Arg::with_name("key").required(true))
                .arg(Arg::with_name("value").required(true))
                .arg(key_format_arg.clone())
                .arg(value_format_arg.clone()),
        )
        .subcommand(
            SubCommand::with_name("remove")
                .about("Remove a key-value pair by its key.")
                .arg(Arg::with_name("key").required(true))
                .arg(key_format_arg.clone()),
        )
        .subcommand(
            SubCommand::with_name("preview")
                .about("Read a pair's value as a document and show it textually.")
                .after_help(
                    "The textual representation of the document is intended for inspection only. \
                    In particular of binary formats, the representation may not be canonical \
                    and may not convert back to binary format without data loss.",
                )
                .arg(Arg::with_name("key").required(true))
                .arg(
                    Arg::with_name("format")
                        .required(true)
                        .possible_values(&DocumentFormat::list()),
                )
                .arg(key_format_arg.clone()),
        )
        .subcommand(SubCommand::with_name("flush").about("Persist changes to database."))
        .subcommand(SubCommand::with_name("exit").about("Exit the inspector."))
}

enum CommandResult {
    Continue,
    Exit,
    Error(anyhow::Error),
}

fn execute_command(database: &mut Database, line: &str) -> anyhow::Result<CommandResult> {
    let args = build_command_args();

    match args.get_matches_from_safe(line.split_ascii_whitespace()) {
        Ok(matches) => match matches.subcommand() {
            ("count", _) => {
                count_command(database);
                Ok(CommandResult::Continue)
            }
            ("get", sub_args) => {
                get_command(database, sub_args.unwrap())?;
                Ok(CommandResult::Continue)
            }
            ("scan", sub_args) => {
                scan_command(database, sub_args.unwrap())?;
                Ok(CommandResult::Continue)
            }
            ("put", sub_args) => {
                put_command(database, sub_args.unwrap())?;
                Ok(CommandResult::Continue)
            }
            ("remove", sub_args) => {
                remove_command(database, sub_args.unwrap())?;
                Ok(CommandResult::Continue)
            }
            ("flush", _) => {
                flush_command(database)?;
                Ok(CommandResult::Continue)
            }
            ("preview", sub_args) => {
                preview_command(database, sub_args.unwrap())?;
                Ok(CommandResult::Continue)
            }
            ("exit", _) => Ok(CommandResult::Exit),
            _ => unreachable!(),
        },
        Err(error) => Ok(CommandResult::Error(anyhow::Error::new(error))),
    }
}

fn encoding_from_args<'a>(args: &'a ArgMatches, name: &str) -> Encoding {
    args.value_of(name)
        .unwrap_or_default()
        .try_into()
        .unwrap_or(Encoding::Utf8)
}

fn text_or_error_from_args<'a>(args: &'a ArgMatches, name: &str) -> anyhow::Result<&'a str> {
    let text = args
        .value_of(name)
        .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 input"))?;
    Ok(text)
}

fn count_command(database: &mut Database) {
    let metadata = database.metadata();
    metadata.key_value_count();

    println!("{}", metadata.key_value_count());
}

fn get_command<'a>(database: &mut Database, args: &'a ArgMatches) -> anyhow::Result<()> {
    let key_encoding = encoding_from_args(args, "key_encoding");
    let value_encoding = encoding_from_args(args, "value_encoding");

    let key = text_or_error_from_args(args, "key")?;
    let key = self::encoding::text_to_binary(key, key_encoding)?;

    let value = database.get(key)?;
    if let Some(value) = value {
        let value = self::encoding::binary_to_text(&value, value_encoding);

        println!("{}", value);
    }

    Ok(())
}

fn scan_command<'a>(database: &mut Database, args: &'a ArgMatches) -> anyhow::Result<()> {
    let key_encoding = encoding_from_args(args, "key_encoding");
    let value_encoding = encoding_from_args(args, "value_encoding");

    let key_start = args.value_of("key_start").unwrap_or_default();
    let key_start = self::encoding::text_to_binary(key_start, key_encoding)?;

    let key_end = args.value_of("key_end").unwrap_or_default();
    let key_end = self::encoding::text_to_binary(key_end, key_encoding)?;

    let cursor = {
        if !key_end.is_empty() {
            database.cursor_range(key_start..key_end)?
        } else {
            database.cursor_range(key_start..)?
        }
    };

    for (key, value) in cursor {
        let key = self::encoding::binary_to_text(&key, key_encoding);
        let value = self::encoding::binary_to_text(&value, value_encoding);

        println!("{}", key);

        if !args.is_present("keys_only") {
            println!("{}", value);
        }
    }

    Ok(())
}

fn put_command<'a>(database: &mut Database, args: &'a ArgMatches) -> anyhow::Result<()> {
    let key_encoding = encoding_from_args(args, "key_encoding");
    let value_encoding = encoding_from_args(args, "value_encoding");

    let key = text_or_error_from_args(args, "key")?;
    let key = self::encoding::text_to_binary(key, key_encoding)?;

    let value = text_or_error_from_args(args, "value")?;
    let value = self::encoding::text_to_binary(value, value_encoding)?;

    database.put(key, value)?;
    println!("OK");

    Ok(())
}

fn remove_command<'a>(database: &mut Database, args: &'a ArgMatches) -> anyhow::Result<()> {
    let key_encoding = encoding_from_args(args, "key_encoding");

    let key = text_or_error_from_args(args, "key")?;
    let key = self::encoding::text_to_binary(key, key_encoding)?;

    database.remove(key)?;
    println!("OK");

    Ok(())
}

fn flush_command(database: &mut Database) -> anyhow::Result<()> {
    database.flush()?;
    println!("OK");
    Ok(())
}

fn preview_command<'a>(database: &mut Database, args: &'a ArgMatches) -> anyhow::Result<()> {
    let key_encoding = encoding_from_args(args, "key_encoding");

    let document_format = args.value_of("format").unwrap_or_default().try_into()?;

    let key = text_or_error_from_args(args, "key")?;
    let key = self::encoding::text_to_binary(key, key_encoding)?;

    let value = database.get(key)?;

    if let Some(value) = value {
        let document = self::encoding::binary_to_document(&value, document_format)?;

        println!("{}", document);
    }

    Ok(())
}
