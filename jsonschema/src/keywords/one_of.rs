use crate::{
    compilation::{compile_validators, context::CompilationContext},
    error::{error, no_error, ErrorIterator, ValidationError},
    keywords::CompilationResult,
    output::BasicOutput,
    paths::{InstancePath, JSONPointer},
    primitive_type::PrimitiveType,
    schema_node::SchemaNode,
    validator::{format_iter_of_validators, PartialApplication, Validate},
    schemas::Discriminator,
};
use serde_json::{Map, Value};
use std::collections::HashMap;

pub(crate) struct OneOfValidator {
    schemas: HashMap<String, SchemaNode>,
    schema_path: JSONPointer,
    discriminator: Option<Discriminator>,
}

impl OneOfValidator {
    #[inline]
    pub(crate) fn compile<'a>(
        schema: &'a Value,
        context: &CompilationContext,
    ) -> CompilationResult<'a> {
        if let Value::Array(items) = schema {
            let keyword_context = context.with_path("oneOf");
            let mut schemas = HashMap::new();
            for (idx, item) in items.iter().enumerate() {
                let item_context = keyword_context.with_path(idx);
                let node = compile_validators(item, &item_context)?;
                schemas.insert(item.get("$ref").expect("fdsa").as_str().expect("fda").to_string(), node);
            }

            Ok(Box::new(OneOfValidator {
                schemas,
                schema_path: keyword_context.into_pointer(),
                discriminator: context.config.discriminator().clone(),
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
    ) -> Option<ErrorIterator<'instance>> {
        if let Some(discriminator) = &self.discriminator {
            if let Some(schema_name) = instance.get(&discriminator.property_name) {
                let schema_ref = discriminator.mapping.get(schema_name.as_str()?)?;
                let node = self.schemas.get(schema_ref)?;
                //return node.err_iter(instance, instance_path);
                return Some(node.validate(instance, instance_path));
            }
        }
        None
    }

    fn get_first_valid(&self, instance: &Value) -> Option<&String> {
        let mut first_valid_idx = None;
        for (idx, node) in &self.schemas {
            if node.is_valid(instance) {
                first_valid_idx = Some(idx);
                break;
            }
        }
        first_valid_idx
    }

    #[allow(clippy::integer_arithmetic)]
    fn are_others_valid(&self, instance: &Value, first_valid_idx: &String) -> bool {
        // `idx + 1` will not overflow, because the maximum possible value there is `usize::MAX - 1`
        // For example we have `usize::MAX` schemas and only the last one is valid, then
        // in `get_first_valid` we enumerate from `0`, and on the last index will be `usize::MAX - 1`
        for (idx, node) in &self.schemas {
            if idx == first_valid_idx {
                continue;
            }
            if node.is_valid(instance) {
                return true;
            }
        }
        false
    }
}

impl Validate for OneOfValidator {
    fn is_valid(&self, instance: &Value) -> bool {
        let first_valid_idx = self.get_first_valid(instance);
        first_valid_idx.map_or(false, |idx| !self.are_others_valid(instance, idx))
    }
    fn validate<'instance>(
        &self,
        instance: &'instance Value,
        instance_path: &InstancePath,
    ) -> ErrorIterator<'instance> {
        if let Some(discriminator_validation) =
            self.get_discriminated_valid(instance, instance_path)
        {
            return discriminator_validation;
        }
        let first_valid_idx = self.get_first_valid(instance);
        if let Some(idx) = first_valid_idx {
            if self.are_others_valid(instance, idx) {
                return error(ValidationError::one_of_multiple_valid(
                    self.schema_path.clone(),
                    instance_path.into(),
                    instance,
                ));
            }
            no_error()
        } else {
            error(ValidationError::one_of_not_valid(
                self.schema_path.clone(),
                instance_path.into(),
                instance,
            ))
        }
    }
    fn apply<'a>(
        &'a self,
        instance: &Value,
        instance_path: &InstancePath,
    ) -> PartialApplication<'a> {
        let mut failures = Vec::new();
        let mut successes = Vec::new();
        for (_, node) in &self.schemas {
            match node.apply_rooted(instance, instance_path) {
                output @ BasicOutput::Valid(..) => successes.push(output),
                output @ BasicOutput::Invalid(..) => failures.push(output),
            };
        }
        if successes.len() == 1 {
            let success = successes.remove(0);
            success.into()
        } else if successes.len() > 1 {
            PartialApplication::invalid_empty(vec!["more than one subschema succeeded".into()])
        } else if !failures.is_empty() {
            failures.into_iter().sum::<BasicOutput<'_>>().into()
        } else {
            unreachable!("compilation should fail for oneOf with no subschemas")
        }
    }
}

impl core::fmt::Display for OneOfValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "oneOf: [{}]",
            format_iter_of_validators(self.schemas.values().map(SchemaNode::validators))
        )
    }
}

#[inline]
pub(crate) fn compile<'a>(
    _: &'a Map<String, Value>,
    schema: &'a Value,
    context: &CompilationContext,
) -> Option<CompilationResult<'a>> {
    Some(OneOfValidator::compile(schema, context))
}

#[cfg(test)]
mod tests {
    use crate::tests_util;
    use serde_json::{json, Value};
    use test_case::test_case;

    #[test_case(&json!({"oneOf": [{"type": "string"}]}), &json!(0), "/oneOf")]
    #[test_case(&json!({"oneOf": [{"type": "string"}, {"maxLength": 3}]}), &json!(""), "/oneOf")]
    fn schema_path(schema: &Value, instance: &Value, expected: &str) {
        tests_util::assert_schema_path(schema, instance, expected)
    }
}
