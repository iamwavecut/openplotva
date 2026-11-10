use std::{env, error::Error, fmt::Write, fs, path::PathBuf};

use sqlx_core::migrate::{MigrationType, resolve_blocking};

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let migrations_dir = manifest_dir.join("../../migrations").canonicalize()?;
    println!("cargo:rerun-if-changed={}", migrations_dir.display());

    let mut output = String::from(
        "pub static MIGRATOR: crate::database::migrate::Migrator = crate::database::migrate::Migrator {\n\
         migrations: std::borrow::Cow::Borrowed(&[\n",
    );
    for (migration, path) in resolve_blocking(&migrations_dir)? {
        let migration_type = match migration.migration_type {
            MigrationType::Simple => "Simple",
            MigrationType::ReversibleUp => "ReversibleUp",
            MigrationType::ReversibleDown => "ReversibleDown",
        };
        let path = path.canonicalize()?;
        let path = path.to_str().ok_or("Migration path is not UTF-8")?;
        writeln!(
            output,
            "crate::database::migrate::Migration {{ version: {}, description: std::borrow::Cow::Borrowed({:?}), \
             migration_type: crate::database::migrate::MigrationType::{}, \
             sql: crate::database::SqlStr::from_static(include_str!({:?})), \
             checksum: std::borrow::Cow::Borrowed(&{:?}), no_tx: {} }},",
            migration.version,
            migration.description,
            migration_type,
            path,
            migration.checksum,
            migration.no_tx,
        )?;
    }
    output.push_str("]), ..crate::database::migrate::Migrator::DEFAULT };\n");
    let destination = PathBuf::from(env::var("OUT_DIR")?).join("migrations.rs");
    fs::write(destination, output)?;
    Ok(())
}
