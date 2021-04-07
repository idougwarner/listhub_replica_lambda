## Run database locally

`docker-compose up`

Check docker-compose.yml file

## Deploy

1. Log in to serverless by `serverless login`
2. Run `serverless deploy`

## CI/CD
CI/CD is already set up so if you push to master branch, it will be automatically deployed.

## Add your own environment variables
- Add the environment variable to both .env and .env.example files
- Register the environment variable to the serverless app. Check the screenshots
https://prnt.sc/116f6vd
- Add the environment variable to serverless.yml

