use crate::converter::ConverterRegistry;

pub(crate) fn register_converters(r: &mut ConverterRegistry) {
    r.register(&crate::converters::text::StdStringToTextDocument::default());
    r.register(&crate::converters::text::AnyToTextDocument::default());
}
