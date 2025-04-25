class StorageService {
    private bucketName: string;
    private storage: any;

    constructor(storage: any, bucketName: string) {
        this.storage = storage;
        this.bucketName = bucketName;
    }

    async downloadCSV(fileName: string, destination: string): Promise<void> {
        const options = {
            destination: destination,
        };

        try {
            await this.storage.bucket(this.bucketName).file(fileName).download(options);
            console.log(`Downloaded ${fileName} to ${destination}`);
        } catch (error) {
            console.error(`Failed to download ${fileName}:`, error);
            throw error;
        }
    }
}

export default StorageService;