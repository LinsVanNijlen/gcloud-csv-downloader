# gcloud-csv-downloader

## Overview
This project is a TypeScript application that downloads CSV files from a specified Google Cloud Storage bucket. It utilizes the Google Cloud Storage client library to interact with the cloud storage service.

## Prerequisites
- Node.js installed on your machine.
- Google Cloud SDK installed and authenticated.
- A Google Cloud project with a storage bucket containing CSV files.

## Installation
1. Clone the repository:
   ```
   git clone <repository-url>
   cd gcloud-csv-downloader
   ```

2. Install the dependencies:
   ```
   npm install
   ```

3. Create a `.env` file based on the `.env.example` file and fill in your Google Cloud project ID and bucket name.

## Usage
To run the application, use the following command:
```
npm start
```

This will execute the `app.ts` file, which initializes the application and downloads the specified CSV files from the Google Cloud bucket.

## Contributing
Feel free to submit issues or pull requests if you have suggestions or improvements for the project.

## License
This project is licensed under the MIT License.