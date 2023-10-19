pub mod console {
    use tabled::builder::Builder;
    use tabled::settings::{Settings, Style};

    pub fn render(headers: Vec<&str>, rows: Vec<Vec<String>>) -> String {
        let mut builder = Builder::new();
        builder.set_header(headers);

        for row in rows {
            builder.push_record(row);
        }

        builder
            .build()
            .with(Settings::default().with(Style::psql()))
            .to_string()
    }
}
