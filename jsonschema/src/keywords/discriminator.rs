use crate::{
    compilation::context::CompilationContext,
    error::{error, ErrorIterator, ValidationError},
    keywords::ref_,
    keywords::CompilationResult,
    paths::{InstancePath, JSONPointer},
    primitive_type::PrimitiveType,
    schema_node::SchemaNode,
    validator::{format_iter_of_validators, PartialApplication, Validate},
};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

pub(crate) struct DiscriminatorValidator {
    schema_path: JSONPointer,
    property_name: String,
    mapping: HashMap<String, SchemaNode>,
}

fn compile_mapping<'a>(
    schema: &'a Value,
    context: &CompilationContext,
) -> Result<SchemaNode, ValidationError<'a>> {
    match schema {
        Value::String(path) => {
            let validator = match ref_::compile(&Map::new(), &schema, &context) {
                Some(Ok(validator)) => validator,
                _ => {
                    return Err(ValidationError::single_type_error(
                        JSONPointer::default(),
                        context.clone().into_pointer(),
                        schema,
                        PrimitiveType::String,
                    ))
                }
            };

            let validators = vec![("$ref".to_string(), validator)];
            Ok(SchemaNode::new_from_keywords(&context, validators, None))
        }
        _ => Err(ValidationError::single_type_error(
            JSONPointer::default(),
            context.clone().into_pointer(),
            schema,
            PrimitiveType::Array,
        )),
    }
}

impl DiscriminatorValidator {
    #[inline]
    pub(crate) fn compile<'a>(
        schema: &'a Value,
        context: &CompilationContext,
    ) -> CompilationResult<'a> {
        if let Value::Object(data) = schema {
            let keyword_context = context.with_path("discriminator");
            let property_name = data
                .get("propertyName")
                .expect("Discriminator must define a propertyName")
                .as_str()
                .expect("Discriminator propertyName must be a string")
                .to_string();
            let mappings = data
                .get("mapping")
                .expect("Discriminator must define a mapping")
                .as_object()
                .expect("Discriminator mapping must be an object");
            let mut mapping = HashMap::new();
            for (idx, item) in mappings {
                let item_context = keyword_context.with_path("test");
                let node = compile_mapping(item, &item_context)?;
                mapping.insert(idx.clone(), node);
            }

            Ok(Box::new(DiscriminatorValidator {
                schema_path: keyword_context.into_pointer(),
                property_name,
                mapping,
            }))
        } else {
            Err(ValidationError::single_type_error(
                JSONPointer::default(),
                context.clone().into_pointer(),
                schema,
                PrimitiveType::Array,
            ))
        }
    }

    fn get_discriminated_valid<'instance>(
        &self,
        instance: &'instance Value,
        instance_path: &InstancePath,
    ) -> ErrorIterator<'instance> {
        if let Some(schema_name) = instance.get(&self.property_name) {
            let node = self
                .mapping
                .get(schema_name.as_str().expect("schema should be a string"))
                .expect("Discriminator mapping must contain a schema for the given property name");
            //return node.err_iter(instance, instance_path);
            return node.validate(instance, instance_path);
        }
        // obviouslyl need a custom error here
        error(ValidationError::one_of_not_valid(
            self.schema_path.clone(),
            instance_path.into(),
            instance,
        ))
    }
}

impl Validate for DiscriminatorValidator {
    fn is_valid(&self, instance: &Value) -> bool {
        false
    }
    fn validate<'instance>(
        &self,
        instance: &'instance Value,
        instance_path: &InstancePath,
    ) -> ErrorIterator<'instance> {
        return self.get_discriminated_valid(instance, instance_path);
    }
    fn apply<'a>(
        &'a self,
        instance: &Value,
        instance_path: &InstancePath,
    ) -> PartialApplication<'a> {
        PartialApplication::invalid_empty(vec!["unimplemented".into()])
    }
}

impl core::fmt::Display for DiscriminatorValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "discriminator: [{}]",
            format_iter_of_validators(self.mapping.values().map(SchemaNode::validators))
        )
    }
}
#[inline]
pub(crate) fn compile<'a>(
    _: &'a Map<String, Value>,
    schema: &'a Value,
    context: &CompilationContext,
) -> Option<CompilationResult<'a>> {
    let resolver = Arc::clone(&context.resolver);
    // shouldn't exit on error, it just means a discriminator isn't present
    let discriminator_schema = match resolver.resolve_fragment(
        context.config.draft().clone(),
        &Url::parse("json-schema:///#/discriminator").expect("valid url"),
        "#/discriminator",
    ) {
        Ok((_, node)) => node,
        Err(_) => {
            return Some(Err(ValidationError::single_type_error(
                JSONPointer::default(),
                context.clone().into_pointer(),
                schema,
                PrimitiveType::Array,
            )))
        }
    };
    match DiscriminatorValidator::compile(&discriminator_schema, context) {
        Ok(validator) => Some(Ok(validator)),
        Err(e) => Some(Err(ValidationError::single_type_error(
            JSONPointer::default(),
            context.clone().into_pointer(),
            schema,
            PrimitiveType::Array,
        ))),
    }
}
