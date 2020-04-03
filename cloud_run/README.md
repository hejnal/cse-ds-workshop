# Setup

Please follow those tutorials: 

https://cloud.google.com/run/docs/quickstarts/prebuilt-deploy
https://cloud.google.com/run/docs/quickstarts/build-and-deploy

# Deploy
```
PROJECT_ID=cse-ds-workshop

# Build the image
gcloud builds submit --tag gcr.io/$PROJECT_ID/helloworld

# Deploy the image
gcloud run deploy --image gcr.io/$PROJECT_ID/helloworld --platform managed
```