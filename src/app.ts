import { Storage } from '@google-cloud/storage';
import * as dotenv from 'dotenv';
import * as fs from 'fs';
import * as path from 'path';
import { Transform } from 'stream';

dotenv.config();

function getRequiredEnvVar(name: string): string {
    const value = process.env[name];
    if (!value) {
        throw new Error(`Required environment variable ${name} is not set`);
    }
    return value;
}

const bucketName = getRequiredEnvVar('BUCKET_NAME');
const storage = new Storage({});

async function listFiles() {
    try {
        const [files] = await storage.bucket(bucketName).getFiles();
        console.log('Files in bucket:');
        files.forEach(file => {
            console.log(`- ${file.name}`);
        });
        return files;
    } catch (error) {
        console.error('Error listing files:', error);
        return [];
    }
}

interface CSVAnalysis {
    rowCount: number;
    columnCount: number;
}

interface CSVMetadata {
    fileName: string;
    rowCount: number;
    columnCount: number;
    sizeInBytes: number;
    createdAt: string;
}

async function analyzeCSV(fileName: string): Promise<CSVAnalysis> {
    const file = storage.bucket(bucketName).file(fileName);
    let rowCount = 0;
    let columnCount = 0;
    let isFirstLine = true;
    
    return new Promise((resolve, reject) => {
        let buffer = '';
        
        file.createReadStream()
            .on('data', (chunk) => {
                buffer += chunk;
                const lines = buffer.split('\n');
                buffer = lines.pop() || ''; // Keep the last partial line in buffer
                
                for (const line of lines) {
                    if (line.trim()) {
                        if (isFirstLine) {
                            columnCount = line.split(',').length;
                            isFirstLine = false;
                        } else {
                            rowCount++;
                        }
                    }
                }
            })
            .on('end', () => {
                // Process any remaining data in buffer
                if (buffer.trim()) {
                    rowCount++;
                }
                resolve({ rowCount, columnCount });
            })
            .on('error', (error) => {
                reject(error);
            });
    });
}

async function getDestinationFolder(analysis: CSVAnalysis): Promise<string> {
    if (analysis.rowCount === 0) {
        return 'empty';
    }
    if (analysis.rowCount < 30) {
        return 'picklists';
    }
    return 'objects';
}

async function generateMetadata(fileName: string, analysis: CSVAnalysis, filePath: string): Promise<CSVMetadata> {
    const stats = await fs.promises.stat(filePath);
    
    return {
        fileName: fileName,
        rowCount: analysis.rowCount,
        columnCount: analysis.columnCount,
        sizeInBytes: stats.size,
        createdAt: new Date().toISOString()
    };
}

class CSVTransform extends Transform {
    private partialChunk: string = '';

    _transform(chunk: Buffer, encoding: BufferEncoding, callback: (error?: Error | null, data?: any) => void) {
        let data = this.partialChunk + chunk.toString();
        let inQuotes = false;
        let result = '';
        
        // Process the data character by character
        for (let i = 0; i < data.length; i++) {
            const char = data[i];
            const nextChar = data[i + 1];
            
            // Toggle quote state
            if (char === '"') {
                inQuotes = !inQuotes;
            }
            
            // Handle different line break scenarios
            if (inQuotes) {
                if (char === '\r' && nextChar === '\n') {
                    // Windows-style line break (\r\n)
                    result += ' ';
                    i++; // Skip the next \n
                } else if (char === '\n' || char === '\r') {
                    // Unix (\n) or old Mac (\r) line breaks
                    result += ' ';
                } else {
                    result += char;
                }
            } else {
                result += char;
            }
        }
        
        // Store the last part of the chunk if we're in quotes
        if (inQuotes) {
            this.partialChunk = result;
            result = '';
        } else {
            this.partialChunk = '';
        }
        
        callback(null, result);
    }

    _flush(callback: (error?: Error | null, data?: any) => void) {
        if (this.partialChunk) {
            callback(null, this.partialChunk);
        } else {
            callback();
        }
    }
}

async function downloadCSV(fileName: string) {
    try {
        const analysis = await analyzeCSV(fileName);
        const folderType = await getDestinationFolder(analysis);
        const baseFileName = path.basename(fileName);
        
        const downloadPath = path.join('./downloads', folderType);
        if (!fs.existsSync(downloadPath)) {
            fs.mkdirSync(downloadPath, { recursive: true });
        }

        const destinationPath = path.join(downloadPath, baseFileName);

        // Check if file exists first
        const [exists] = await storage.bucket(bucketName).file(fileName).exists();
        if (!exists) {
            console.error(`File ${fileName} does not exist in bucket ${bucketName}`);
            return;
        }

        // Create transform stream to handle line breaks
        const transformStream = new CSVTransform();

        // Create write stream
        const writeStream = fs.createWriteStream(destinationPath);

        // Download and process the file
        await new Promise<void>((resolve, reject) => {
            const readStream = storage.bucket(bucketName).file(fileName).createReadStream();
            readStream
                .pipe(transformStream)
                .pipe(writeStream)
                .on('finish', () => resolve())
                .on('error', reject);
        });

        // Generate and save metadata
        const metadata = await generateMetadata(fileName, analysis, destinationPath);
        
        // Create metadata folder within the category folder
        const metadataFolderPath = path.join(downloadPath, 'metadata');
        if (!fs.existsSync(metadataFolderPath)) {
            fs.mkdirSync(metadataFolderPath, { recursive: true });
        }

        // Save metadata file in the metadata subfolder
        const metadataPath = path.join(
            metadataFolderPath,
            `${path.parse(baseFileName).name}.json`
        );
        
        await fs.promises.writeFile(
            metadataPath, 
            JSON.stringify(metadata, null, 2)
        );

        console.log(`Downloaded ${fileName} to ${destinationPath} (${analysis.rowCount} rows, ${analysis.columnCount} columns)`);
        console.log(`Generated metadata file: ${metadataPath}`);
    } catch (error) {
        console.error(`Failed to process ${fileName}:`, error);
    }
}

async function downloadAllCSVs() {
    const files = await listFiles();
    const csvFiles = files.filter(file => path.extname(file.name).toLowerCase() === '.csv');
    
    if (csvFiles.length === 0) {
        console.log('No CSV files found in the bucket');
        return;
    }

    console.log(`Found ${csvFiles.length} CSV files. Starting download...`);
    
    for (const file of csvFiles) {
        await downloadCSV(file.name);
    }
    
    console.log('Finished downloading all CSV files');
}

async function main() {
    console.log('Starting CSV download process...');
    await downloadAllCSVs();
}

main();