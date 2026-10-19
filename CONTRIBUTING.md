# Development workflow

For a concrete problem, open an issue describing the observed behavior and
acceptance criteria before starting new implementation. Check for an existing
issue first. Keep one independently reviewable problem in each PR, with logical
commits as the work progresses.

Run checks appropriate to the change and distinguish unit tests, real service
integration, actual model execution and later decision outcomes. Link the issue
in the PR using `Fixes #N`, describe the final behavior and report limitations.
After verification, merge the PR into main and confirm that its issue is closed.
The owner has authorized agents to perform this workflow in this repository.

Preserve raw observations, state, offsets, evaluation records and credentials.
Runtime data and local API configuration are ignored by Git. Do not call a
container's running state proof of a working agent, or synthetic test results
proof of investment performance.
