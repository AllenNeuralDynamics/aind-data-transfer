# aind-data-transfer

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)

Tools for transferring large data to and between cloud storage providers.

## Installation

To upload data to aws s3, you may need to install and configure `awscli`. To upload data to gcp, you may need to install and configure `gsutil`.

### Imaging

- Run `pip install -e .[imaging]`
- Run `./post_install.sh`

### Ephys

- Run `pip install -e .[ephys]`

### Full

- Run `pip install -e .[full]`
- Run `./post_install.sh`

#### Development

- Run `pip install -e .[dev]`
- Run `./post_install.sh`

## Contributing

### Linters and testing

There are several libraries used to run linters, check documentation, and run tests.

- Please test your changes using the **coverage** library, which will run the tests and log a coverage report:

```
coverage run -m unittest discover && coverage report
```

- Use **interrogate** to check that modules, methods, etc. have been documented thoroughly:

```
interrogate .
```

- Use **flake8** to check that code is up to standards (no unused imports, etc.):

```
flake8 .
```

- Use **black** to automatically format the code into PEP standards:

```
black .
```

- Use **isort** to automatically sort import statements:

```
isort .
```

### Pull requests

For internal members, please create a branch. For external members, please fork the repo and open a pull request from the fork. We'll primarily use [Angular](https://github.com/angular/angular/blob/main/CONTRIBUTING.md#commit) style for commit messages. Roughly, they should follow the pattern:
```
<type>(<scope>): <short summary>
```

where scope (optional) describes the packages affected by the code changes and type (mandatory) is one of:

- **build**: Changes that affect the build system or external dependencies (example scopes: pyproject.toml, setup.py)
- **ci**: Changes to our CI configuration files and scripts (examples: .github/workflows/ci.yml)
- **docs**: Documentation only changes
- **feat**: A new feature
- **fix**: A bug fix
- **perf**: A code change that improves performance
- **refactor**: A code change that neither fixes a bug nor adds a feature
- **test**: Adding missing tests or correcting existing tests
