import ffmpeg from 'fluent-ffmpeg';
import AWS from 'aws-sdk';
import fs from 'fs-extra';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const OUTPUT_DIR = path.join(__dirname, 'output');
const TRANSCRIPT_FILE = path.join(__dirname, 'transcript.json');
const S3_BUCKET = 'saumik-test-bucket';
const S3_PATH = 'ingest-live/';
const REGION = 'eu-west-1';

let tsSegmentsUploaded = 0;
const uploadedFiles = new Set();
let transcriptInitialized = false;

AWS.config.update({
  accessKeyId: '',
  secretAccessKey: '',
  sessionToken: '',
  region: REGION
});

const s3 = new AWS.S3();
ffmpeg.setFfmpegPath('/usr/bin/ffmpeg');
fs.ensureDirSync(OUTPUT_DIR);

function logWithTimestamp(message: any) {
  console.log(`[${new Date().toISOString()}] ${message}`);
}

const generateLiveSessionDirectory = () => Date.now().toString();

async function uploadToS3(filePath: string, s3Key: string) {
  try {
    logWithTimestamp(`Uploading: ${filePath} to S3 as ${s3Key}`);
    const fileData = await fs.readFile(filePath);
    const contentType = filePath.endsWith('.ts') ? 'video/MP2T' : 
                        filePath.endsWith('.jpg') ? 'image/jpeg' : 
                        'application/vnd.apple.mpegurl';

    await s3.putObject({
      Bucket: S3_BUCKET,
      Key: `${S3_PATH}${s3Key}`,
      Body: fileData,
      ContentType: contentType
    }).promise();
    
    logWithTimestamp(`✅ Uploaded: ${s3Key}`);
  } catch (error) {
    console.error(`❌ Error uploading ${s3Key}:`, error);
  }
}

// Clear transcript file
async function clearTranscriptFile() {
  try {
    const initialTranscript = {
      words: [],
      speakers: {
        "speaker1": { name: "Speaker 1" },
        "speaker2": { name: "Speaker 2" }
      },
      paragraphs: {},
      realtime: { status: "ACTIVE" },
      title: "Transcript",
    };
    
    // Remove existing file if it exists
    if (fs.existsSync(TRANSCRIPT_FILE)) {
      await fs.unlink(TRANSCRIPT_FILE);
    }
    
    // Write fresh structure
    await fs.writeFile(TRANSCRIPT_FILE, JSON.stringify(initialTranscript, null, 2));
    transcriptInitialized = false;
    logWithTimestamp("Created fresh transcript.json file");
  } catch (error) {
    console.error('❌ Error clearing transcript file:', error);
  }
}

async function updateTranscriptionSegment(liveSessionId: string, segmentCount: number) {
  try {
    let transcriptData: Record<string, any> = {
      words: [],
      speakers: {
        "speaker1": { name: "Speaker 1" },
        "speaker2": { name: "Speaker 2" }
      },
      paragraphs: {},
      realtime: { status: "ACTIVE" },
      title: "Default Title"
    };
    
    if (fs.existsSync(TRANSCRIPT_FILE)) {
      try {
        const fileContent = await fs.readFile(TRANSCRIPT_FILE, 'utf-8');
        const parsed = JSON.parse(fileContent);
        if (parsed && typeof parsed === 'object') {
          transcriptData = parsed;
          
          if (!Array.isArray(transcriptData.words)) {
            transcriptData.words = [];
          }
        }
      } catch (error) {
        console.error('❌ Error reading transcript file:', error);
      }
    }

    const speaker1Words = [`word`, `from`, `segment`, `${segmentCount}`];
    const speaker2Words = [`example`, `${segmentCount}`];
    
    const newWords = [];
    let currentTime = segmentCount * 1000;
    
    for (const word of speaker1Words) {
      newWords.push({
        duration: 150, 
        time: currentTime, 
        value: word, 
        speaker: "speaker1"
      });
      currentTime += 150;
    }
    
    currentTime += 100;
    
    for (const word of speaker2Words) {
      newWords.push({
        duration: 150, 
        time: currentTime, 
        value: word, 
        speaker: "speaker2"
      });
      currentTime += 150;
    }
    
    if (Array.isArray(transcriptData.words)) {
      newWords.forEach(word => {
        transcriptData.words.push(word);
      });
    } else {
      transcriptData.words = newWords;
    }
    
    transcriptData.updateTime = new Date().toISOString();
    
    await fs.writeFile(
      TRANSCRIPT_FILE, 
      JSON.stringify(transcriptData, null, 2)
    );
    
    await uploadToS3(TRANSCRIPT_FILE, `${liveSessionId}/transcript.json`);
    logWithTimestamp(`📤 Updated transcript for segment ${segmentCount}, total words: ${transcriptData.words.length}`);
  } catch (error) {
    console.error('❌ Error updating transcript:', error);
  }
}

function startSegmenting(inputFile: string, liveSessionId: string) {
  const liveSessionDir = path.join(OUTPUT_DIR, liveSessionId);
  fs.ensureDirSync(liveSessionDir);
  
  return new Promise<void>((resolve, reject) => {
    ffmpeg(inputFile)
      .output(path.join(liveSessionDir, 'stream.m3u8'))
      .outputOptions([
        '-codec:v libx264',
        '-codec:a aac',
        '-preset fast',
        '-b:v 800k',
        '-hls_time 5',
        '-hls_list_size 0',
        `-hls_segment_filename ${path.join(liveSessionDir, 'segment_%03d.ts')}`
      ])
      .on('end', async () => {
        logWithTimestamp(`✅ FFmpeg segmentation complete`);
        await uploadToS3(path.join(liveSessionDir, 'stream.m3u8'), `${liveSessionId}/stream.m3u8`);
        pollForNewSegments(liveSessionId);
        resolve();
      })
      .on('error', (err) => {
        console.error(`❌ FFmpeg error:`, err);
        reject(err);
      })
      .run();
  });
}

function pollForNewSegments(liveSessionId: string) {
  const liveSessionDir = path.join(OUTPUT_DIR, liveSessionId);
  let processing = false;
  
  const intervalId = setInterval(async () => {
    if (processing) return;
    processing = true;
    
    try {
      const files = await fs.readdir(liveSessionDir);
      const tsFiles = files.filter(file => file.endsWith('.ts')).sort();
      
      for (const file of tsFiles) {
        const filePath = path.join(liveSessionDir, file);
        if (uploadedFiles.has(filePath)) continue;
        
        const segmentIndex = file.match(/\d+/)?.[0].padStart(6, '0') || '000000';
        const s3Key = `${liveSessionId}/highres.${segmentIndex}.ts`;
        
        await uploadToS3(filePath, s3Key);
        uploadedFiles.add(filePath);
        tsSegmentsUploaded++;
        
        if (tsSegmentsUploaded % 3 === 0) {
          await updateTranscriptionSegment(liveSessionId, tsSegmentsUploaded);
        }
      }
      
      if (tsFiles.length > 0 && !files.some(f => f.startsWith('segment_') && !uploadedFiles.has(path.join(liveSessionDir, f)))) {
        logWithTimestamp('All segments processed');
        clearInterval(intervalId);
      }
    } catch (error) {
      console.error(`❌ Error polling directory:`, error);
    }
    
    processing = false;
  }, 5000);
}

async function main() {
  try {
    logWithTimestamp('🚀 Starting Stream to S3...');
    await clearTranscriptFile();
    
    const liveSessionId = generateLiveSessionDirectory();
    await startSegmenting('/home/saumik_nondi/projects/ffmpeg/video.mp4', liveSessionId);
  } catch (err) {
    console.error('❌ Error:', err);
  }
}
main();

export {};