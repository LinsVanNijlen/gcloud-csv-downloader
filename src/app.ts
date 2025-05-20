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
const excludeSystemHeaders = getRequiredEnvVar('EXCLUDE_SYSTEM_HEADERS') === 'true';
const removeLowCardinality = getRequiredEnvVar('REMOVE_LOW_CARDINALITY') === 'true';
const storage = new Storage({});

const SYSTEM_HEADERS = [
    // Creation related
    'createdby',
    'createdbyname',
    'createdbyyominame',
    'createdonbehalfby',
    'createdonbehalfbyname',
    'createdonbehalfbyyominame',

    // Modification related
    'modifiedby',
    'modifiedbyname',
    'modifiedbyyominame',
    'modifiedon',
    'modifiedonbehalfby',
    'modifiedonbehalfbyname',
    'modifiedonbehalfbyyominame',

    // Owner related
    'ownerid',
    'owneridname',
    'owneridtype',
    'owneridyominame',
    'owningbusinessunit',
    'owningbusinessunitname',
    'owningteam',
    'owninguser',

    // Status related
    'statecode',
    'statuscode',

    // System fields
    'importsequencenumber',
    'overriddencreatedon',
    'timezoneruleversionnumber',
    'utcconversiontimezonecode',
    'versionnumber',
    'export_date'
];

async function listFiles() {
    try {
        console.log(bucketName);
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

async function analyzeUniqueValues(fileName: string): Promise<Map<string, number>> {
    const file = storage.bucket(bucketName).file(fileName);
    const uniqueValues = new Map<string, Set<string>>();
    let headerProcessed = false;
    let headers: string[] = [];
    
    return new Promise((resolve, reject) => {
        let buffer = '';
        let inQuotes = false;
        let currentField = '';
        let currentRow: string[] = [];

        file.createReadStream()
            .on('data', (chunk) => {
                const data = chunk.toString();
                
                for (let i = 0; i < data.length; i++) {
                    const char = data[i];

                    if (char === '"') {
                        inQuotes = !inQuotes;
                    } else if (char === ',' && !inQuotes) {
                        currentRow.push(currentField);
                        currentField = '';
                    } else if ((char === '\n' || char === '\r') && !inQuotes) {
                        if (char === '\r' && data[i + 1] === '\n') {
                            i++; // Skip the \n in \r\n
                        }
                        
                        currentRow.push(currentField);
                        
                        if (!headerProcessed) {
                            headers = currentRow.map(h => h.trim());
                            headers.forEach(header => {
                                uniqueValues.set(header, new Set());
                            });
                            headerProcessed = true;
                        } else {
                            headers.forEach((header, index) => {
                                const value = (currentRow[index] || '').trim().replace(/^"|"$/g, '');
                                if (value) {
                                    uniqueValues.get(header)?.add(value);
                                }
                            });
                        }
                        
                        currentRow = [];
                        currentField = '';
                    } else {
                        currentField += char;
                    }
                }
                buffer = currentField;
            })
            .on('end', () => {
                // Convert Sets to counts
                const uniqueCounts = new Map<string, number>();
                uniqueValues.forEach((values, header) => {
                    uniqueCounts.set(header, values.size);
                });
                resolve(uniqueCounts);
            })
            .on('error', reject);
    });
}

async function analyzeColumns(
    filePath: string, 
    totalRows: number,
    progressBar?: cliProgress.SingleBar,
    excludeSystemHeaders: boolean = true
): Promise<ColumnMetadata[]> {
    return new Promise((resolve, reject) => {
        const columns: Map<string, ColumnMetadata> = new Map();
        let headerProcessed = false;
        let valueMap: Map<string, Set<string>> = new Map();
        let valueCount: Map<string, Map<string, number>> = new Map();
        let processedRows = 0;

        const fileStream = fs.createReadStream(filePath);
        let buffer = '';
        let inQuotes = false;
        let currentField = '';
        let currentRow: string[] = [];

        progressBar?.update(30, { stage: 'Analyzing columns (0%)' });

        fileStream
            .on('data', (chunk) => {
                const data = chunk.toString();
                
                for (let i = 0; i < data.length; i++) {
                    const char = data[i];

                    if (char === '"') {
                        inQuotes = !inQuotes;
                    } else if (char === ',' && !inQuotes) {
                        currentRow.push(currentField);
                        currentField = '';
                    } else if ((char === '\n' || char === '\r') && !inQuotes) {
                        if (char === '\r' && data[i + 1] === '\n') {
                            i++; // Skip the \n in \r\n
                        }
                        
                        currentRow.push(currentField);
                        
                        if (!headerProcessed) {
                            // Process headers
                            currentRow.forEach(header => {
                                const cleanHeader = header.trim();
                                if (!excludeSystemHeaders || !SYSTEM_HEADERS.includes(cleanHeader.toLowerCase())) {
                                    columns.set(cleanHeader, {
                                        name: cleanHeader,
                                        dataType: 'Unknown',
                                        totalCount: 0,
                                        nonNullCount: 0,
                                        uniqueCount: 0,
                                        maxLength: 0,
                                        minLength: undefined,
                                        topValues: []
                                    });
                                    valueMap.set(cleanHeader, new Set());
                                    valueCount.set(cleanHeader, new Map());
                                }
                            });
                            headerProcessed = true;
                        } else {
                            // Process row data
                            Array.from(columns.keys()).forEach((header, index) => {
                                const value = (currentRow[index] || '').trim().replace(/^"|"$/g, '');
                                const metadata = columns.get(header)!;
                                
                                metadata.totalCount++;

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

                            processedRows++;
                            if (processedRows % 1000 === 0) {
                                const progress = 35 + ((processedRows / totalRows) * 45);
                                const percentage = Math.round((processedRows / totalRows) * 100);
                                progressBar?.update(progress, { 
                                    stage: `Analyzing columns (${percentage}%) [${processedRows}/${totalRows}]` 
                                });
                            }
                        }
                        
                        currentRow = [];
                        currentField = '';
                    } else {
                        currentField += char;
                    }
                }
                buffer = currentField;
            })
            .on('end', () => {
                // Process any remaining data
                if (buffer.trim()) {
                    currentRow.push(buffer);
                    // Process final row if not empty
                    if (currentRow.length > 0) {
                        Array.from(columns.keys()).forEach((header, index) => {
                            const value = (currentRow[index] || '').trim().replace(/^"|"$/g, '');
                            const metadata = columns.get(header)!;
                            
                            metadata.totalCount++;

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
                }

                // Finalize metadata processing
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

async function generateMetadata(
    fileName: string, 
    analysis: CSVAnalysis, 
    filePath: string, 
    progressBar?: cliProgress.SingleBar,
    excludeSystemHeaders: boolean = true
): Promise<CSVMetadata> {
    const stats = await fs.promises.stat(filePath);
    progressBar?.update(30, { stage: 'Reading creation dates' });
    
    const latestCreation = await findLatestCreationDate(filePath);
    const columns = await analyzeColumns(filePath, analysis.rowCount, progressBar, excludeSystemHeaders);
    
    progressBar?.update(90, { stage: 'Preparing final metadata' });
    
    return {
        fileName: fileName,
        rowCount: analysis.rowCount,
        columnCount: columns.length,
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
        let latestDate = new Date(0);
        let headerProcessed = false;
        let createdonIndex = -1;

        const fileStream = fs.createReadStream(filePath);
        let buffer = '';

        fileStream
            .on('data', (chunk) => {
                buffer += chunk.toString();
                const lines = buffer.split('\n');
                buffer = lines.pop() || '';

                for (const line of lines) {
                    if (!headerProcessed) {
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

// First modify the CSVTransform class to accept low cardinality columns and track processed rows
class CSVTransform extends Transform {
    private partialChunk: string = '';
    private isFirstChunk: boolean = true;
    private headers: string[] = [];
    private headerIndices: number[] = [];
    private lowCardinalityColumns: Set<string>;
    private rowCount: number = 0;

    constructor(lowCardinalityColumns: string[] = []) {
        super();
        this.lowCardinalityColumns = new Set(lowCardinalityColumns);
    }

    _transform(chunk: Buffer, encoding: BufferEncoding, callback: (error?: Error | null, data?: any) => void) {
        let data = this.partialChunk + chunk.toString();
        let inQuotes = false;
        let result = '';
        
        if (this.isFirstChunk) {
            const newlineIndex = data.indexOf('\n');
            if (newlineIndex === -1) {
                this.partialChunk = data;
                callback();
                return;
            }

            const headerRow = data.substring(0, newlineIndex);
            this.headers = this.parseCSVLine(headerRow);
            
            this.headerIndices = this.headers
                .map((header, index) => {
                    const isSystemHeader = SYSTEM_HEADERS.includes(header.toLowerCase()) && excludeSystemHeaders;
                    const isLowCardinality = this.lowCardinalityColumns.has(header) && removeLowCardinality;
                    return (!isSystemHeader && !isLowCardinality) ? index : -1;
                })
                .filter(index => index !== -1);

            const filteredHeaders = this.headerIndices.map(i => this.headers[i]);
            result = filteredHeaders.join(',') + '\n';
            
            data = data.substring(newlineIndex + 1);
            this.isFirstChunk = false;
        }

        let currentField = '';
        let currentLine: string[] = [];
        
        for (let i = 0; i < data.length; i++) {
            const char = data[i];
            
            if (char === '"') {
                inQuotes = !inQuotes;
                currentField += char;
            } else if (char === ',' && !inQuotes) {
                currentLine.push(currentField);
                currentField = '';
            } else if ((char === '\n' || char === '\r') && !inQuotes) {
                if (char === '\r' && data[i + 1] === '\n') {
                    i++;
                }
                
                currentLine.push(currentField);
                const filteredLine = this.headerIndices.map(index => currentLine[index] || '');
                result += filteredLine.join(',') + '\n';
                
                if (!this.isFirstChunk) {
                    this.rowCount++;
                }
                
                currentLine = [];
                currentField = '';
            } else if (inQuotes && (char === '\n' || char === '\r')) {
                currentField += ' ';
                if (char === '\r' && data[i + 1] === '\n') {
                    i++;
                }
            } else {
                currentField += char;
            }
        }

        if (inQuotes || currentField || currentLine.length > 0) {
            this.partialChunk = currentField;
            if (currentLine.length > 0) {
                this.partialChunk = currentLine.join(',') + ',' + this.partialChunk;
            }
        } else {
            this.partialChunk = '';
        }

        callback(null, result);
    }

    _flush(callback: (error?: Error | null, data?: any) => void) {
        if (this.partialChunk) {
            const currentLine = this.parseCSVLine(this.partialChunk);
            const filteredLine = this.headerIndices.map(index => currentLine[index] || '');
            callback(null, filteredLine.join(','));
        } else {
            callback();
        }
    }

    private parseCSVLine(line: string): string[] {
        const fields: string[] = [];
        let currentField = '';
        let inQuotes = false;

        for (let i = 0; i < line.length; i++) {
            const char = line[i];
            if (char === '"') {
                inQuotes = !inQuotes;
                currentField += char;
            } else if (char === ',' && !inQuotes) {
                fields.push(currentField);
                currentField = '';
            } else {
                currentField += char;
            }
        }
        
        fields.push(currentField.trim());
        
        return fields;
    }

    getRowCount(): number {
        return this.rowCount;
    }
}

// Then modify the downloadCSV function to use the low cardinality columns and updated row count
async function downloadCSV(fileName: string) {
    try {
        console.log(`\nProcessing ${fileName}...`);
        
        console.log('Analyzing unique values per column...');
        const uniqueCounts = await analyzeUniqueValues(fileName);

        const lowCardinalityColumns = Array.from(uniqueCounts.entries())
            .filter(([header, count]) => 
                count <= 1 && (!SYSTEM_HEADERS.includes(header.toLowerCase()))
            );
        
        if (lowCardinalityColumns.length > 0 && removeLowCardinality) {
            console.log('\nRemoving columns with constant values (1 or fewer unique values):');
            console.log('─'.repeat(50));
            console.log('Column Name'.padEnd(35) + 'Unique Values');
            console.log('─'.repeat(50));
            
            lowCardinalityColumns.forEach(([header, count]) => {
                console.log(header.padEnd(35) + count);
            });
            console.log('─'.repeat(50) + '\n');
        } else {
            console.log('\nNo constant value columns to remove.\n');
        }

        const progressBars = new cliProgress.MultiBar({
            clearOnComplete: false,
            hideCursor: true,
            format: '{bar} | {percentage}% | {stage}'
        }, cliProgress.Presets.shades_classic);

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

        const [exists] = await storage.bucket(bucketName).file(fileName).exists();
        if (!exists) {
            console.error(`File ${fileName} does not exist in bucket ${bucketName}`);
            return;
        }

        const transformStream = new CSVTransform(
            lowCardinalityColumns.map(([header]) => header)
        );

        const writeStream = fs.createWriteStream(destinationPath);

        const [fileMetadata] = await storage.bucket(bucketName).file(fileName).getMetadata();
        const fileSize = parseInt(fileMetadata.size);
        let downloadedBytes = 0;

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

        metadataBar.update(30);
        // Update analysis with correct row count
        analysis.rowCount = transformStream.getRowCount();
        
        const metadata = await generateMetadata(
            fileName, 
            analysis, 
            destinationPath, 
            metadataBar,
            excludeSystemHeaders
        );
        metadataBar.update(60);
        
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

// Add these new functions before the main function:

async function generateCsvHeaders(objectsPath: string, picklistsPath: string): Promise<{
    content: string;
    path: string;
}> {
    const csvHeadersPath = path.join('./downloads', 'csv_headers.ts');
    const csvHeadersStart = `import type { FromProperties } from '../utils/types.js';

export const inboundCsvHeaders = {`;

    const csvHeadersEnd = `} as const;
    
export type InboundCsvFileName = keyof typeof inboundCsvHeaders;
export function InboundCsvFileName<T extends InboundCsvFileName>(dbCsvFileName: T): T {
  return dbCsvFileName;
}
export function InboundCsvFileNameMap<T extends { [key in InboundCsvFileName]?: any }>(dbCsvFileNameMap: T): T {
  return dbCsvFileNameMap;
}

export type InboundCsvFileHeader<TFileName extends InboundCsvFileName> = keyof FromProperties<(typeof inboundCsvHeaders)[TFileName]>;

export type InboundCsvFileRow<TFileName extends InboundCsvFileName> = FromProperties<(typeof inboundCsvHeaders)[TFileName]>;`;

    let csvHeadersContent = '';

    for (const folderPath of [objectsPath, picklistsPath]) {
        if (!fs.existsSync(folderPath)) continue;
        
        const files = await fs.promises.readdir(folderPath);
        for (const file of files) {
            if (path.extname(file) === '.json') {
                const metadata = JSON.parse(
                    await fs.promises.readFile(path.join(folderPath, file), 'utf8')
                );

                const baseFileName = path.basename(metadata.fileName, '.csv').replace(/^.*\//, '');
                const headers = metadata.columns.map((col: { name: any; }) => `'${col.name}'`);
                csvHeadersContent += `  ${baseFileName}: [\n    ${headers.join(',\n    ')}\n  ] as const,\n`;
            }
        }
    }

    return {
        content: csvHeadersStart + '\n' + csvHeadersContent + '\n' + csvHeadersEnd,
        path: csvHeadersPath
    };
}

async function collectRelationshipFields(objectsPath: string, picklistsPath: string, summaryPath: string): Promise<Array<{
    sourceEntity: string;
    sourceField: string;
}>> {
    const relationshipFields: Array<{ sourceEntity: string; sourceField: string }> = [];
    
    for (const folderPath of [objectsPath, picklistsPath]) {
        if (!fs.existsSync(folderPath)) continue;
        
        const files = await fs.promises.readdir(folderPath);
        for (const file of files) {
            if (path.extname(file) === '.json') {
                const metadata = JSON.parse(
                    await fs.promises.readFile(path.join(folderPath, file), 'utf8')
                );

                if (metadata.rowCount > 0) {
                    const fileType = metadata.rowCount >= 30 ? 'Object' : 'Picklist';
                    const baseFileName = path.basename(metadata.fileName, '.csv').replace(/^.*\//, '');
                    const identifier = findIdentifierField(metadata);
                    
                    const summary = [
                        baseFileName,
                        fileType,
                        metadata.rowCount,
                        new Date(metadata.latestCreation).toISOString().split('T')[0],
                        identifier || ''
                    ].join(',');
                    
                    await fs.promises.appendFile(summaryPath, summary + '\n');
                    
                    const foreignKeyFields = metadata.columns.filter((col: { dataType: string; name: string; }) => 
                        col.dataType === 'GUID' && col.name !== identifier
                    );
                    
                    for (const foreignKeyField of foreignKeyFields) {
                        relationshipFields.push({
                            sourceEntity: baseFileName,
                            sourceField: foreignKeyField.name
                        });
                    }
                }
            }
        }
    }

    return relationshipFields;
}

async function processRelationships(
    relationshipFields: Array<{ sourceEntity: string; sourceField: string }>,
    objectsPath: string,
    picklistsPath: string
): Promise<void> {
    
        // Sort relationships by source entity and then field name
        relationshipFields.sort((a, b) => 
            a.sourceEntity.localeCompare(b.sourceEntity) || 
            a.sourceField.localeCompare(b.sourceField)
        );
        
        // Create a relationships CSV file
        const relationshipsPath = path.join('./downloads', 'relationships.csv');
        await fs.promises.writeFile(
            relationshipsPath, 
            'SourceEntity,SourceField,TargetEntity,TargetField\n'
        );
        
        // Create a map of entities and their identifiers
        const entityIdentifiers = new Map<string, string>();
        
        // First pass: collect all entity names and their identifiers
        for (const folderPath of [objectsPath, picklistsPath]) {
            if (!fs.existsSync(folderPath)) continue;
            
            const files = await fs.promises.readdir(folderPath);
            for (const file of files) {
                if (path.extname(file) === '.json') {
                    const metadata = JSON.parse(
                        await fs.promises.readFile(path.join(folderPath, file), 'utf8')
                    );
                    const baseFileName = path.basename(metadata.fileName, '.csv').replace(/^.*\//, '');
                    const identifier = findIdentifierField(metadata);
                    if (identifier) {
                        entityIdentifiers.set(baseFileName.toLowerCase(), identifier);
                    }
                }
            }
        }
        
        for (const rel of relationshipFields) {
            let foundMatch = false;

            const baseFieldName = rel.sourceField.toLowerCase().endsWith('id') ? 
                rel.sourceField.slice(0, -2) : rel.sourceField;
            
            // Get source field metadata and top values
            // Try objects directory first, then picklists
            let sourceMetadataPath = path.join('./downloads/objects/metadata', `${rel.sourceEntity}.json`);
            if (!fs.existsSync(sourceMetadataPath)) {
                sourceMetadataPath = path.join('./downloads/picklists/metadata', `${rel.sourceEntity}.json`);
                if (!fs.existsSync(sourceMetadataPath)) {
                    console.log(`\nSkipping relationship: ${rel.sourceEntity}.${rel.sourceField} - Metadata file not found`);
                    continue;
                }
            }

            const sourceMetadata = JSON.parse(
                await fs.promises.readFile(sourceMetadataPath, 'utf8')
            );

            const sourceFieldMetadata = sourceMetadata.columns.find(
                (col: { name: string }) => col.name === rel.sourceField
            );
            const topValues = sourceFieldMetadata?.topValues || [];

            console.log(`\nAnalyzing relationship: ${rel.sourceEntity}.${rel.sourceField}`);
            if (topValues.length > 0) {
                console.log('Top values from source field:');
                console.log('─'.repeat(50));
                console.log('Value'.padEnd(38) + 'Count');
                console.log('─'.repeat(50));
                topValues.forEach(({value, count}: { value: string; count: number }) => {
                    console.log(`${value.padEnd(38)}${count}`);
                });
                console.log('─'.repeat(50));
            }
            
            // Look for matching target entity and its identifier
            const targetEntity = entityIdentifiers.has(baseFieldName.toLowerCase()) ? 
                baseFieldName : '';
            const targetField = targetEntity ? 
                entityIdentifiers.get(baseFieldName.toLowerCase()) : '';

            if (targetEntity) {

                // Find the CSV file for the target entity
                const targetCsvPath = path.join('./downloads/objects', `${targetEntity}.csv`);
                const picklistCsvPath = path.join('./downloads/picklists', `${targetEntity}.csv`);

                let csvPath = '';
                if (fs.existsSync(targetCsvPath)) {
                    csvPath = targetCsvPath;
                } else if (fs.existsSync(picklistCsvPath)) {
                    csvPath = picklistCsvPath;
                }

                if (csvPath) {
                    // Read the CSV file and check all identifiers
                    const fileStream = fs.createReadStream(csvPath);
                    let buffer = '';
                    let isFirstLine = true;
                    let headers: string[] = [];
                    let identifierIndex = -1;

                    const parseCSVLine = (line: string): string[] => {
                        const fields: string[] = [];
                        let currentField = '';
                        let inQuotes = false;

                        for (let i = 0; i < line.length; i++) {
                            const char = line[i];
                            if (char === '"') {
                                inQuotes = !inQuotes;
                            } else if (char === ',' && !inQuotes) {
                                fields.push(currentField.trim());
                                currentField = '';
                            } else {
                                currentField += char;
                            }
                        }
                        fields.push(currentField.trim());
                        return fields.map(field => field.replace(/^"|"$/g, '')); // Remove surrounding quotes
                    };

                    console.log(`\nFound matching entity: ${targetEntity}`);
                    console.log(`Checking ${targetField} values against source field values...`);

                    for await (const chunk of fileStream) {
                        buffer += chunk.toString();
                        const lines = buffer.split('\n');
                        buffer = lines.pop() || ''; // Keep the last partial line

                        if (isFirstLine && lines.length > 0) {
                            headers = parseCSVLine(lines[0]);
                            identifierIndex = headers.findIndex(h => h === targetField);
                            isFirstLine = false;
                        }

                        if (identifierIndex !== -1) {
                            for (const line of lines) {
                                if (!line.trim()) continue;
                                const values = parseCSVLine(line);
                                const identifierValue = values[identifierIndex];
                                
                                if (identifierValue && topValues.some((tv: { value: string; }) => tv.value === identifierValue)) {
                                    console.log(`✓ Found matching identifier value: ${identifierValue}`);
                                    foundMatch = true;
                                    break;
                                }
                            }

                            if (foundMatch) break;
                        }
                    }

                    if (foundMatch) {
                        // Write complete relationship only when match is found
                        await fs.promises.appendFile(
                            relationshipsPath,
                            `${rel.sourceEntity},${rel.sourceField},${targetEntity},${targetField}\n`
                        );
                    }
                } else {
                    console.log(`✗ No CSV file found for target entity: ${targetEntity}`);
                    // Write to relationships CSV with only source information even when target file is missing
                    await fs.promises.appendFile(
                        relationshipsPath,
                        `${rel.sourceEntity},${rel.sourceField},,\n`
                    );
                }
            }

            if (!foundMatch) {
                // Get list of all CSV files from both directories
                const objectFiles = fs.existsSync('./downloads/objects') ? 
                    await fs.promises.readdir('./downloads/objects') : [];
                const picklistFiles = fs.existsSync('./downloads/picklists') ? 
                    await fs.promises.readdir('./downloads/picklists') : [];

                // Get file sizes and create sorted list
                const allFiles = await Promise.all([
                    ...objectFiles.map(async file => ({
                        path: path.join('./downloads/objects', file),
                        size: (await fs.promises.stat(path.join('./downloads/objects', file))).size,
                        entity: path.basename(file, '.csv')
                    })),
                    ...picklistFiles.map(async file => ({
                        path: path.join('./downloads/picklists', file),
                        size: (await fs.promises.stat(path.join('./downloads/picklists', file))).size,
                        entity: path.basename(file, '.csv')
                    }))
                ]);

                // Sort by file size (ascending)
                allFiles.sort((a, b) => a.size - b.size);

                console.log('\nChecking remaining CSV files for matches (ordered by size):');
                for (const file of allFiles) {
                    // Skip source entity and already checked target
                    if (file.entity === targetEntity) continue;
                    
                    // Get metadata for potential target
                    const metadataPath = path.join(
                        path.dirname(file.path),
                        'metadata',
                        `${file.entity}.json`
                    );
                    
                    if (!fs.existsSync(metadataPath)) continue;

                    const targetMetadata = JSON.parse(
                        await fs.promises.readFile(metadataPath, 'utf8')
                    );
                    const potentialIdentifier = findIdentifierField(targetMetadata);
                    
                    if (!potentialIdentifier) continue;

                    console.log(`\nChecking ${file.entity}.${potentialIdentifier}...`);

                    // Read the CSV file and check all identifiers
                    const fileStream = fs.createReadStream(file.path);
                    let buffer = '';
                    let isFirstLine = true;
                    let headers: string[] = [];
                    let identifierIndex = -1;

                    for await (const chunk of fileStream) {
                        buffer += chunk.toString();
                        const lines = buffer.split('\n');
                        buffer = lines.pop() || '';

                        if (isFirstLine && lines.length > 0) {
                            headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
                            identifierIndex = headers.findIndex(h => h === potentialIdentifier);
                            isFirstLine = false;
                        }

                        if (identifierIndex !== -1) {
                            for (const line of lines) {
                                if (!line.trim()) continue;
                                const values = line.split(',').map(v => v.trim().replace(/^"|"$/g, ''));
                                const identifierValue = values[identifierIndex];
                                
                                if (identifierValue && topValues.some((tv: { value: string; }) => tv.value === identifierValue)) {
                                    console.log(`✓ Found matching identifier value: ${identifierValue}`);
                                    console.log(`✓ Discovered relationship: ${rel.sourceEntity}.${rel.sourceField} -> ${file.entity}.${potentialIdentifier}`);
                                    
                                    await fs.promises.appendFile(
                                        relationshipsPath,
                                        `${rel.sourceEntity},${rel.sourceField},${file.entity},${potentialIdentifier}\n`
                                    );
                                    
                                    foundMatch = true;
                                    break;
                                }
                            }

                            if (foundMatch) break;
                        }
                    }

                    if (foundMatch) break;
                }

                if (!foundMatch) {
                    console.log('✗ No matches found in any remaining files');
                    await fs.promises.appendFile(
                        relationshipsPath,
                        `${rel.sourceEntity},${rel.sourceField},,\n`
                    );
                }
            }
        }
        
        console.log('─'.repeat(90));
        console.log(`Found ${relationshipFields.length} potential relationship fields`);
        console.log(`Relationship data saved to: ${relationshipsPath}`);

}

async function generateSourceConfigs(objectsPath: string, picklistsPath: string): Promise<void> {
    const sourceConfigPath = path.join('./downloads', 'source-configs');
    if (!fs.existsSync(sourceConfigPath)) {
        fs.mkdirSync(sourceConfigPath, { recursive: true });
    }

    for (const folderPath of [objectsPath, picklistsPath]) {
        if (!fs.existsSync(folderPath)) continue;
        
        const files = await fs.promises.readdir(folderPath);
        for (const file of files) {
            if (path.extname(file) === '.json') {
                const metadata = JSON.parse(
                    await fs.promises.readFile(path.join(folderPath, file), 'utf8')
                );

                if (metadata.rowCount > 0) {
                    const baseFileName = path.basename(metadata.fileName, '.csv').replace(/^.*\//, '');
                    const identifier = findIdentifierField(metadata);
                    
                    if (identifier) {
                        // Find fields with max length > 250
                        const longTextFields = metadata.columns
                            .filter((col: { maxLength: number; }) => col.maxLength && col.maxLength > 250)
                            .map((col: { name: any; }) => col.name);

                        // Format the longTextFields array
                        const longTextFieldsStr = longTextFields.length > 0 
                            ? `    longTextFields: [\n        ${longTextFields.map((field: any) => `'${field}'`).join(',\n        ')}\n    ],`
                            : '    longTextFields: [],';
                        
                        // Determine if we need division settings
                        const needsDivision = metadata.rowCount > 500000;
                        
                        const csvImportOptions = needsDivision ? 
                            `    csvImportOptions: {
      delimiter: ',',
      primaryKeyFields: ['${identifier}'],
      encoding: 'ISO-8859-15',
      divideInSections: 10,
      divideInSectionsByField: '${identifier}',
    },` :
                            `    csvImportOptions: {
      delimiter: ',',
      primaryKeyFields: ['${identifier}'],
      encoding: 'ISO-8859-15',
    },`;

                        const configContent = `import { inboundSourceConfig } from '../../utils/migration-utils.js';
import { InboundCsvFileName, inboundCsvHeaders } from '../csv-headers.js';

const filename = InboundCsvFileName('${baseFileName}');

export const ${baseFileName}Source = inboundSourceConfig({
    filename: filename,
    headers: inboundCsvHeaders[filename],
    primaryKey: 'pk',
    inboundFieldPrefix: 'Dyn',
    fieldTypeOverrides: {},
${longTextFieldsStr}
    keyResolvers: {
        pk: ['${identifier}'],
        ${baseFileName}Key: async ({ sourceRecord }) => \`\${sourceRecord.${identifier}}\`,
    },
    filter: () => true,
${csvImportOptions}
});
`;
                        const configFilePath = path.join(sourceConfigPath, `${baseFileName}.ts`);
                        await fs.promises.writeFile(configFilePath, configContent);
                        console.log(`Generated source config: ${configFilePath}`);
                    }
                }
            }
        }
    }
}

async function generateTargetConfigs(objectsPath: string, picklistsPath: string): Promise<void> {
    const targetConfigPath = path.join('./downloads', 'target-configs');
    if (!fs.existsSync(targetConfigPath)) {
        fs.mkdirSync(targetConfigPath, { recursive: true });
    }

    // Read relationships file
    const relationshipsPath = path.join('./downloads', 'relationships.csv');
    const relationshipsContent = await fs.promises.readFile(relationshipsPath, 'utf8');
    const relationships = relationshipsContent
        .split('\n')
        .slice(1) // Skip header
        .filter(line => line.trim())
        .map(line => {
            const [sourceEntity, sourceField, targetEntity, targetField] = line.split(',');
            return { sourceEntity, sourceField, targetEntity, targetField };
        });

    // Group relationships by source entity
    const relationshipsByEntity = relationships.reduce((acc, rel) => {
        if (!acc[rel.sourceEntity]) {
            acc[rel.sourceEntity] = [];
        }
        if (rel.targetEntity && rel.targetField) {
            acc[rel.sourceEntity].push(rel);
        }
        return acc;
    }, {} as Record<string, typeof relationships>);

    for (const folderPath of [objectsPath, picklistsPath]) {
        if (!fs.existsSync(folderPath)) continue;
        
        const files = await fs.promises.readdir(folderPath);
        for (const file of files) {
            if (path.extname(file) === '.json') {
                const metadata = JSON.parse(
                    await fs.promises.readFile(path.join(folderPath, file), 'utf8')
                );

                if (metadata.rowCount > 0) {
                    const baseFileName = path.basename(metadata.fileName, '.csv').replace(/^.*\//, '');
                    const entityRelationships = relationshipsByEntity[baseFileName] || [];
                    
                    // Generate imports
                    const imports = [`import { lookupRef, LookupRef, targetConfig, targetSourceConfig } from "../../../migration-core";`,
                        `import { assignInboundFields, defaultInboundTargetConfig, inboundConfigValidator } from "../../utils/migration-utils";`,
                        `import { ${baseFileName}Source } from "../source-configs/${baseFileName}";`,
                        `import { SfdcRow } from "../table-types";`
                    ];

                    // Add imports for related entities
                    const uniqueTargetEntities = [...new Set(entityRelationships.map(r => r.targetEntity))];
                    uniqueTargetEntities.forEach(targetEntity => {
                        // Skip importing own class to avoid circular dependencies
                        if (targetEntity !== baseFileName) {
                            imports.push(`import { Dyn__${targetEntity}__c } from "./${targetEntity}";`);
                        }
                    });

                    // Generate class definition
                    const classProps = [`  public Dyn__${baseFileName}Key__c?: string = undefined;`];
                    entityRelationships.forEach(rel => {
                        classProps.push(`  public Dyn__${rel.sourceField}__c: LookupRef<Dyn__${rel.targetEntity}__c> | null | undefined = undefined;`);
                    });

                    const classDefinition = `\nexport class Dyn__${baseFileName}__c extends SfdcRow {\n${classProps.join('\n')}\n}`;

                    // Generate lookup assignments
                    const lookupAssignments = entityRelationships.map(rel => 
                        `    record.Dyn__${rel.sourceField}__c = lookupRef(\`\${${baseFileName}.${rel.sourceField}}\`);`
                    );

                    // Generate target config
                    const targetConfigContent = `\nexport const Dyn__${baseFileName}__c_Target = targetConfig({
  ...defaultInboundTargetConfig,
  targetTableName: "Dyn__${baseFileName}__c",
  sources: {
    ${baseFileName}: targetSourceConfig({
      sourceConfig: ${baseFileName}Source,
      sourceKey: "${baseFileName}Key",
    }),
  },
}).setRowDigester<Dyn__${baseFileName}__c>({
  validate: inboundConfigValidator([${baseFileName}Source], []),
  digest: async (context) => {
    const {
      sources: { ${baseFileName} },
      key,
    } = context;

    const record: Dyn__${baseFileName}__c = {
      ...(await assignInboundFields(${baseFileName}Source, ${baseFileName})),
    };

${lookupAssignments.join('\n')}

    record.Dyn__${baseFileName}Key__c = key;
    return record;
  }
})`;

                    const configContent = [...imports, classDefinition, targetConfigContent].join('\n');
                    const configFilePath = path.join(targetConfigPath, `${baseFileName}.ts`);
                    await fs.promises.writeFile(configFilePath, configContent);
                    console.log(`Generated target config: ${configFilePath}`);
                }
            }
        }
    }
}

// Update the main function to use the new functions:
async function main() {
    console.log('Starting CSV download process...');
    //await downloadAllCSVs();
    
    console.log('\nGenerating summary...');
    const summaryPath = path.join('./downloads', 'summary.csv');
    await fs.promises.writeFile(summaryPath, 'Filename,FileType,RecordCount,LastCreationDate,Identifier\n');
    
    const objectsPath = path.join('./downloads/objects/metadata');
    const picklistsPath = path.join('./downloads/picklists/metadata');

    // Generate CSV headers
    const csvHeaders = await generateCsvHeaders(objectsPath, picklistsPath);
    await fs.promises.writeFile(csvHeaders.path, csvHeaders.content);

    // Collect relationship fields and generate summary
    const relationshipFields = await collectRelationshipFields(objectsPath, picklistsPath, summaryPath);
    console.log(`Summary saved to: ${summaryPath}`);
    
    // Process relationships
    /*console.log('\nAnalyzing non-identifier GUID fields (potential relationships):');
    if (relationshipFields.length > 0) {
        await processRelationships(relationshipFields, objectsPath, picklistsPath);
    } else {
        console.log('No non-identifier GUID fields found.');
    }*/

    // Generate source configs
    console.log('\nGenerating source configurations...');
    await generateSourceConfigs(objectsPath, picklistsPath);

    // Generate target configs
    console.log('\nGenerating target configurations...');
    await generateTargetConfigs(objectsPath, picklistsPath);
}

// Helper function to find the GUID field with most unique values
function findIdentifierField(metadata: CSVMetadata): string {
    // Filter columns to find GUID fields
    const guidColumns = metadata.columns.filter(col => col.dataType === 'GUID');
    
    // If no GUID fields, return empty string
    if (guidColumns.length === 0) return '';
    
    // First sort by unique count in descending order
    guidColumns.sort((a, b) => b.uniqueCount - a.uniqueCount);
    
    // Get the highest unique count
    const maxUniqueCount = guidColumns[0].uniqueCount;
    
    // Find all columns with this max count
    const candidateColumns = guidColumns.filter(col => col.uniqueCount === maxUniqueCount);
    
    // If only one column has the highest count, return it
    if (candidateColumns.length === 1) {
        return candidateColumns[0].name;
    }
    
    // Extract filename from the metadata
    const baseFileName = path.basename(metadata.fileName, '.csv').replace(/^.*\//, '');
    
    // Look for columns containing the file name
    const fileNameMatch = candidateColumns.find(col => 
        col.name.toLowerCase().includes(baseFileName.toLowerCase())
    );
    
    // If found a match with filename, return it
    if (fileNameMatch) {
        return fileNameMatch.name;
    }
    
    // Fallback: return the first column with highest unique count
    return candidateColumns[0].name;
}

main();