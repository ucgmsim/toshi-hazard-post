# toshi-hazard-post


[![pypi](https://img.shields.io/pypi/v/toshi-hazard-post.svg)](https://pypi.org/project/toshi-hazard-post/)
[![python](https://img.shields.io/pypi/pyversions/toshi-hazard-post.svg)](https://pypi.org/project/toshi-hazard-post/)
[![Build Status](https://github.com/gns-science/toshi-hazard-post/actions/workflows/dev.yml/badge.svg)](https://github.com/gns-science/toshi-hazard-post/actions/workflows/dev.yml)
[![codecov](https://codecov.io/gh/gns-science/toshi-hazard-post/branch/main/graphs/badge.svg)](https://codecov.io/github/gns-science/toshi-hazard-post)



Hazard post-processing pipeline as serverless AWS infrastructure.


* Documentation: <https://gns-science.github.io/toshi-hazard-post>
* GitHub: <https://github.com/gns-science/toshi-hazard-post>
* PyPI: <https://pypi.org/project/toshi-hazard-post/>
* Free software: AGPL


## Features

* Run hazard aggregation task LOCAL or DOCKER or LAMBDA `python3 -m toshi_hazard_post.cli`

## Usage

### BATCH Deployment (for BATCH)

```
docker build . -t toshi-hazard-post
docker run --rm -it toshi-hazard-post -s bash
/app$> python toshi_hazard_post.cli --help

```
 TODO -> DOCKER_BUILD.md

### Serverless Deployment (for Lambda now, BATCH later)

In order to deploy the example, you need to run the following command:

```
$ serverless deploy
```

After running deploy, you should see output similar to:

```bash
Deploying aws-python-project to stage dev (us-east-1)

✔ Service deployed to stack aws-python-project-dev (112s)

functions:
  hello: aws-python-project-dev-hello (1.5 kB)
```

### Invocation

After successful deployment, you can invoke the deployed function by using the following command:

```bash
serverless invoke --function hello
```

Which should result in response similar to the following:

```json
{
    "statusCode": 200,
    "body": "{\"message\": \"Go Serverless v3.0! Your function executed successfully!\", \"input\": {}}"
}
```

### Local development

You can invoke your function locally by using the following command:

```bash
serverless invoke local --function hello
```

Which should result in response similar to the following:

```
{
    "statusCode": 200,
    "body": "{\"message\": \"Go Serverless v3.0! Your function executed successfully!\", \"input\": {}}"
}
```

### Bundling dependencies

In case you would like to include third-party dependencies, you will need to use a plugin called `serverless-python-requirements`. You can set it up by running the following command:

```bash
serverless plugin install -n serverless-python-requirements
```

Running the above will automatically add `serverless-python-requirements` to `plugins` section in your `serverless.yml` file and add it as a `devDependency` to `package.json` file. The `package.json` file will be automatically created if it doesn't exist beforehand. Now you will be able to add your dependencies to `requirements.txt` file (`Pipfile` and `pyproject.toml` is also supported but requires additional configuration) and they will be automatically injected to Lambda package during build process. For more details about the plugin's configuration, please refer to [official documentation](https://github.com/UnitedIncome/serverless-python-requirements).

## Credits

This package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter) and the [waynerv/cookiecutter-pypackage](https://github.com/waynerv/cookiecutter-pypackage) project template.
