# pg-tbus

## 0.1.9

### Patch Changes

- Performance and sql caching

## 0.1.8

### Patch Changes

- Correctly propagate properties from task declaration to task definition

## 0.1.7

### Patch Changes

- Allow to emit tasks to different queues

## 0.1.6

### Patch Changes

- Ability to define target queue per task declaration

## 0.1.5

### Patch Changes

- Ability to declare task and use it in definitions

## 0.1.4

### Patch Changes

- a6d83f6: Singleton task per queue
- d8ac5ec: Add refill impl for task workers to reduce latency
- 13d9c38: Get serializable state of tbus

## 0.1.3

### Patch Changes

- Upgrade node-batcher

## 0.1.2

### Patch Changes

- Fix expire_in_time

## 0.1.1

### Patch Changes

- 2264812: Use node-batcher for resolving tasks

## 0.1.0

### Minor Changes

- 5963c8c: Replace pg-boss with own task worker

## 0.0.10

### Patch Changes

- 0c10ce7: Switch to safe-stable-stringify

## 0.0.9

### Patch Changes

- Expose query commands + optimalisations

## 0.0.8

### Patch Changes

- Dynamic handler config

## 0.0.7

### Patch Changes

- Cleanup + stop fixes

## 0.0.6

### Patch Changes

- 64a401c: Task options (retry etc)

## 0.0.5

### Patch Changes

- 8dad120: Optimize latency on same instance

## 0.0.4

### Patch Changes

- 9399f79: Position index and initial cursor

## 0.0.3

### Patch Changes

- d2bace6: Documentation

## 0.0.2

### Patch Changes

- Remove unnecessary files from published package
