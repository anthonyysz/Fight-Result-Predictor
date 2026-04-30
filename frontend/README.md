# Frontend

This is the deployed React frontend for the fight-result-predictor project.

## Environment variables

- Local development can use `frontend/.env`
- Production builds do not read local files at runtime
- `REACT_APP_API_BASE_URL` must be supplied at build time in AWS CodeBuild so the
  built bundle points at the Elastic Beanstalk backend host

If `REACT_APP_API_BASE_URL` is not provided, the app falls back to
`http://127.0.0.1:8000` for desktop development only.
