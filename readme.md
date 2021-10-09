## Sourced Repo DynamoDB

- [ ] figure out how to fit with generic single table design - possibly accept hash and sort key as config params
      NOTE: right now the old package uses 2 separate tables for events and snapshots, this is bad
- [ ] figure out how to access latest events for an entity by timestamp with starts_with() query generically - I think you'll need an ID no matter what
- [ ] figure out how to access latest snapshots for entity by timestamp with starts_with() query generically
- [ ] figure out if it makes sense to keep table references as entity references in single table design for quick puts / gets as any or to type as reference objects to ddb actions
- [ ] use batch put operations with retries OR a dynamodb transaction for committing