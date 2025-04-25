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

interface ColumnMetadata {
    name: string;
    dataType: 'GUID' | 'String' | 'Date' | 'Number' | 'Boolean' | 'Unknown';
    totalCount: number;
    nonNullCount: number;
    uniqueCount: number;
    // String specific
    maxLength?: number;
    minLength?: number;
    // Number specific
    min?: number;
    max?: number;
    average?: number;
    // Date specific
    earliestDate?: string;
    latestDate?: string;
    // Additional insights
    isAllUpperCase?: boolean;
    containsMultipleLines?: boolean;
    hasSpecialCharacters?: boolean;
    topValues?: Array<{ value: string; count: number }>;
}

interface CSVMetadata {
    fileName: string;
    rowCount: number;
    columnCount: number;
    sizeInBytes: number;
    latestCreation: string;
    columns: ColumnMetadata[];
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

async function analyzeColumns(filePath: string): Promise<ColumnMetadata[]> {
    return new Promise((resolve, reject) => {
        const columns: Map<string, ColumnMetadata> = new Map();
        let headerProcessed = false;
        let valueMap: Map<string, Set<string>> = new Map();
        let valueCount: Map<string, Map<string, number>> = new Map();

        const fileStream = fs.createReadStream(filePath);
        let buffer = '';

        fileStream
            .on('data', (chunk) => {
                buffer += chunk.toString();
                const lines = buffer.split('\n');
                buffer = lines.pop() || '';

                for (const line of lines) {
                    if (!headerProcessed) {
                        // Initialize column metadata
                        const headers = line.split(',').map(h => h.trim());
                        headers.forEach(header => {
                            columns.set(header, {
                                name: header,
                                dataType: 'Unknown',
                                totalCount: 0,
                                nonNullCount: 0,
                                uniqueCount: 0,
                                maxLength: 0,
                                minLength: undefined,
                                topValues: []
                            });
                            valueMap.set(header, new Set());
                            valueCount.set(header, new Map());
                        });
                        headerProcessed = true;
                        continue;
                    }

                    // Process column values
                    const values = line.split(',');
                    values.forEach((value, index) => {
                        const header = Array.from(columns.keys())[index];
                        if (!header) return;

                        const metadata = columns.get(header)!;
                        metadata.totalCount++;

                        value = value.trim().replace(/"/g, '');
                        if (value) {
                            metadata.nonNullCount++;
                            valueMap.get(header)?.add(value);

                            // Update value frequency
                            const frequencyMap = valueCount.get(header)!;
                            frequencyMap.set(value, (frequencyMap.get(value) || 0) + 1);

                            // Update string metrics
                            metadata.maxLength = Math.max(metadata.maxLength!, value.length);
                            metadata.minLength = metadata.minLength === undefined ? 
                                value.length : Math.min(metadata.minLength, value.length);

                            // Detect data type
                            if (metadata.dataType === 'Unknown') {
                                if (/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(value)) {
                                    metadata.dataType = 'GUID';
                                } else if (!isNaN(Number(value))) {
                                    metadata.dataType = 'Number';
                                } else if (!isNaN(Date.parse(value))) {
                                    metadata.dataType = 'Date';
                                } else if (value.toLowerCase() === 'true' || value.toLowerCase() === 'false') {
                                    metadata.dataType = 'Boolean';
                                } else {
                                    metadata.dataType = 'String';
                                }
                            }

                            // Additional insights
                            metadata.isAllUpperCase = value === value.toUpperCase();
                            metadata.containsMultipleLines = value.includes('\n') || value.includes('\r');
                            metadata.hasSpecialCharacters = /[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>/?]+/.test(value);
                        }
                    });
                }
            })
            .on('end', () => {
                // Finalize metadata
                for (const [header, metadata] of columns) {
                    const uniqueValues = valueMap.get(header)!;
                    metadata.uniqueCount = uniqueValues.size;

                    // Get top 5 most frequent values
                    const frequencies = Array.from(valueCount.get(header)!.entries())
                        .sort((a, b) => b[1] - a[1])
                        .slice(0, 5)
                        .map(([value, count]) => ({ value, count }));
                    metadata.topValues = frequencies;

                    if (metadata.dataType === 'Number') {
                        const numbers = Array.from(uniqueValues).map(Number).filter(n => !isNaN(n));
                        if (numbers.length > 0) {
                            metadata.min = Math.min(...numbers);
                            metadata.max = Math.max(...numbers);
                            metadata.average = numbers.reduce((a, b) => a + b) / numbers.length;
                        }
                    } else if (metadata.dataType === 'Date') {
                        const dates = Array.from(uniqueValues)
                            .map(d => new Date(d))
                            .filter(d => !isNaN(d.getTime()));
                        if (dates.length > 0) {
                            metadata.earliestDate = new Date(Math.min(...dates.map(d => d.getTime()))).toISOString();
                            metadata.latestDate = new Date(Math.max(...dates.map(d => d.getTime()))).toISOString();
                        }
                    }
                }

                resolve(Array.from(columns.values()));
            })
            .on('error', reject);
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

async function findLatestCreationDate(filePath: string): Promise<string> {
    return new Promise((resolve, reject) => {
        let latestDate = new Date(0); // Start with earliest possible date
        let headerProcessed = false;
        let createdonIndex = -1;

        const fileStream = fs.createReadStream(filePath);
        let buffer = '';

        fileStream
            .on('data', (chunk) => {
                buffer += chunk.toString();
                const lines = buffer.split('\n');
                buffer = lines.pop() || ''; // Keep the last partial line

                for (const line of lines) {
                    if (!headerProcessed) {
                        // Find the index of the createdon column
                        const headers = line.split(',');
                        createdonIndex = headers.findIndex(h => h.trim() === 'createdon');
                        headerProcessed = true;
                        continue;
                    }

                    if (createdonIndex !== -1) {
                        const columns = line.split(',');
                        if (columns[createdonIndex]) {
                            const dateStr = columns[createdonIndex].trim().replace(/"/g, '');
                            if (dateStr) {
                                const date = new Date(dateStr);
                                if (!isNaN(date.getTime()) && date > latestDate) {
                                    latestDate = date;
                                }
                            }
                        }
                    }
                }
            })
            .on('end', () => {
                // Process any remaining data in buffer
                if (buffer && createdonIndex !== -1) {
                    const columns = buffer.split(',');
                    if (columns[createdonIndex]) {
                        const dateStr = columns[createdonIndex].trim().replace(/"/g, '');
                        if (dateStr) {
                            const date = new Date(dateStr);
                            if (!isNaN(date.getTime()) && date > latestDate) {
                                latestDate = date;
                            }
                        }
                    }
                }
                resolve(latestDate.toISOString());
            })
            .on('error', reject);
    });
}

async function generateMetadata(fileName: string, analysis: CSVAnalysis, filePath: string): Promise<CSVMetadata> {
    const stats = await fs.promises.stat(filePath);
    const latestCreation = await findLatestCreationDate(filePath);
    const columns = await analyzeColumns(filePath);
    
    return {
        fileName: fileName,
        rowCount: analysis.rowCount,
        columnCount: analysis.columnCount,
        sizeInBytes: stats.size,
        latestCreation: latestCreation,
        columns: columns
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