import { Storage } from '@google-cloud/storage';
import * as dotenv from 'dotenv';
import * as fs from 'fs';
import * as path from 'path';
import { Transform } from 'stream';
import * as cliProgress from 'cli-progress';

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

// First modify the function signature to accept the progress bar
async function analyzeColumns(
    filePath: string, 
    totalRows: number,
    progressBar?: cliProgress.SingleBar
): Promise<ColumnMetadata[]> {
    return new Promise((resolve, reject) => {
        const columns: Map<string, ColumnMetadata> = new Map();
        let headerProcessed = false;
        let valueMap: Map<string, Set<string>> = new Map();
        let valueCount: Map<string, Map<string, number>> = new Map();
        let processedRows = 0;

        const fileStream = fs.createReadStream(filePath);
        let buffer = '';

        // Update progress bar with initial status
        progressBar?.update(30, { stage: 'Analyzing columns (0%)' });

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
                        progressBar?.update(35, { stage: 'Headers processed' });
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

                    // Update progress every 1000 rows
                    processedRows++;
                    if (processedRows % 1000 === 0) {
                        const progress = 35 + ((processedRows / totalRows) * 45);
                        const percentage = Math.round((processedRows / totalRows) * 100);
                        progressBar?.update(progress, { 
                            stage: `Analyzing columns (${percentage}%) [${processedRows}/${totalRows}]` 
                        });
                    }
                }
            })
            .on('end', () => {
                progressBar?.update(80, { stage: 'Finalizing column analysis' });
                
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
                            const { min, max, sum } = numbers.reduce((acc, num) => ({
                                min: Math.min(acc.min, num),
                                max: Math.max(acc.max, num),
                                sum: acc.sum + num
                            }), { min: Infinity, max: -Infinity, sum: 0 });

                            metadata.min = min;
                            metadata.max = max;
                            metadata.average = sum / numbers.length;
                        }
                    } else if (metadata.dataType === 'Date') {
                        const dates = Array.from(uniqueValues)
                            .map(d => new Date(d))
                            .filter(d => !isNaN(d.getTime()));
                        if (dates.length > 0) {
                            const { earliest, latest } = dates.reduce((acc, date) => ({
                                earliest: date < acc.earliest ? date : acc.earliest,
                                latest: date > acc.latest ? date : acc.latest
                            }), { earliest: new Date(8640000000000000), latest: new Date(-8640000000000000) });

                            metadata.earliestDate = earliest.toISOString();
                            metadata.latestDate = latest.toISOString();
                        }
                    }
                }

                progressBar?.update(85, { stage: 'Column analysis complete' });
                resolve(Array.from(columns.values()));
            })
            .on('error', reject);
    });
}

// Then update the generateMetadata function to pass the total rows
async function generateMetadata(
    fileName: string, 
    analysis: CSVAnalysis, 
    filePath: string, 
    progressBar?: cliProgress.SingleBar
): Promise<CSVMetadata> {
    const stats = await fs.promises.stat(filePath);
    progressBar?.update(30, { stage: 'Reading creation dates' });
    
    const latestCreation = await findLatestCreationDate(filePath);
    const columns = await analyzeColumns(filePath, analysis.rowCount, progressBar);
    
    progressBar?.update(90, { stage: 'Preparing final metadata' });
    
    return {
        fileName: fileName,
        rowCount: analysis.rowCount,
        columnCount: analysis.columnCount,
        sizeInBytes: stats.size,
        latestCreation: latestCreation,
        columns: columns
    };
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
        console.log(`\nProcessing ${fileName}...`);
        
        // Create progress bars
        const progressBars = new cliProgress.MultiBar({
            clearOnComplete: false,
            hideCursor: true,
            format: '{bar} | {percentage}% | {stage}'
        }, cliProgress.Presets.shades_classic);

        // Create individual progress bars for each stage
        const downloadBar = progressBars.create(100, 0, { stage: 'Downloading file          ' });
        const analysisBar = progressBars.create(100, 0, { stage: 'Analyzing file structure  ' });
        const metadataBar = progressBars.create(100, 0, { stage: 'Generating metadata       ' });

        const analysis = await analyzeCSV(fileName);
        analysisBar.update(100);

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

        // Get file size for progress calculation
        const [fileMetadata] = await storage.bucket(bucketName).file(fileName).getMetadata();
        const fileSize = parseInt(fileMetadata.size);
        let downloadedBytes = 0;

        // Download with progress
        await new Promise<void>((resolve, reject) => {
            const readStream = storage.bucket(bucketName).file(fileName).createReadStream();
            readStream
                .on('data', chunk => {
                    downloadedBytes += chunk.length;
                    const progress = (downloadedBytes / fileSize) * 100;
                    downloadBar.update(Math.min(progress, 100));
                })
                .pipe(transformStream)
                .pipe(writeStream)
                .on('finish', () => resolve())
                .on('error', reject);
        });

        downloadBar.update(100);

        // Generate metadata with progress updates
        metadataBar.update(30);
        const metadata = await generateMetadata(fileName, analysis, destinationPath, metadataBar);
        metadataBar.update(60);
        
        // Save metadata
        const metadataFolderPath = path.join(downloadPath, 'metadata');
        if (!fs.existsSync(metadataFolderPath)) {
            fs.mkdirSync(metadataFolderPath, { recursive: true });
        }

        const metadataPath = path.join(
            metadataFolderPath,
            `${path.parse(baseFileName).name}.json`
        );
        
        await fs.promises.writeFile(
            metadataPath, 
            JSON.stringify(metadata, null, 2)
        );

        metadataBar.update(100);
        progressBars.stop();

        console.log(`✓ Completed processing ${fileName}`);
        console.log(`  └─ CSV saved to: ${destinationPath}`);
        console.log(`  └─ Metadata saved to: ${metadataPath}`);
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
    console.log('─'.repeat(50));
    
    for (const [index, file] of csvFiles.entries()) {
        console.log(`\nProcessing file ${index + 1}/${csvFiles.length}`);
        await downloadCSV(file.name);
    }
    
    console.log('\n' + '─'.repeat(50));
    console.log('Finished downloading all CSV files');
}

async function main() {
    console.log('Starting CSV download process...');
    await downloadAllCSVs();
}

main();