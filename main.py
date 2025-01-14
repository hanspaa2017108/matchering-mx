import json
import matchering as mg
import os
from pydub import AudioSegment
from io import BytesIO
import boto3
import requests
import time
import botocore.exceptions
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Environment variables
bucket_name = os.getenv("bucket_name")
region_name = os.getenv("region_name")
webhook_url = os.getenv("webhook_url")

# Create AWS clients
s3_client = boto3.client('s3', region_name=region_name)

# Lambda constants
LAMBDA_STATIC_REFERENCE_WAV_PATH = "static/audio/reference.wav"
LOCAL_REFERENCE_PATH = "/tmp/reference.wav"
LOCAL_INSTRUMENTAL_PATH = "/tmp/instrumental.wav"
LOCAL_FINAL_FILE = None
LOCAL_VOCALS_PATH = None

def get_dynamic_s3_paths(track_id):
    """Construct dynamic paths for instrumental files based on track ID."""
    TRACK_FILENAMES = {
        1: "pop_track1.wav",
        2: "norteno_track2.wav",
        3: "urbano_track3.wav"
    }
    track_filename = TRACK_FILENAMES.get(track_id, None)
    if not track_filename:
        raise ValueError(f"Invalid trackID: {track_id}")
    return f"static/audio/{track_filename}"

def initialize_s3_paths(track_id):
    """Initialize the global path for instrumental files dynamically."""
    global LAMBDA_STATIC_INSTRUMENTAL_WAV_PATH
    LAMBDA_STATIC_INSTRUMENTAL_WAV_PATH = get_dynamic_s3_paths(track_id)

def upload_file_to_s3(local_file, bucket, key):
    """Upload the specified file to S3."""
    try:
        logger.info(f"Uploading {local_file} to s3://{bucket}/{key}...")
        s3_client.upload_file(local_file, bucket, key)
        logger.info(f"File {local_file} uploaded successfully.")
    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")
        raise

def wait_for_file(bucket, key, s3_client, timeout=10, interval=5):
    """Wait for the specified file to become available in S3."""
    elapsed_time = 0
    while elapsed_time < timeout:
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            logger.info(f"File {key} is now available in bucket {bucket}.")
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.info(f"Waiting for file {key} in bucket {bucket}...")
                time.sleep(interval)
                elapsed_time += interval
            else:
                raise e
    logger.error(f"File {key} did not become available within the timeout period.")
    return False

def download_file_from_s3(bucket, key, local_output_file, s3_client):
    """Download the specified file from S3 to local storage."""
    try:
        logger.info(f"Downloading {key} from S3...")
        s3_client.download_file(bucket, key, local_output_file)
        logger.info(f"File downloaded to {local_output_file}")
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        raise

def process_files(vocals_path, instrumental_path, output_path, reference_path):
    """Main processing function for mixing and mastering."""
    logger.info("Processing files for mixing and mastering...")
    logger.info(f"Vocals Path: {vocals_path}")
    logger.info(f"Instrumental Path: {instrumental_path}")
    logger.info(f"Output Path: {output_path}")
    logger.info(f"Reference Path: {reference_path}")

    try:
        mixed_audio = mix_tracks(vocals_path, instrumental_path)
        if mixed_audio is None:
            raise ValueError("Failed to mix audio tracks.")

        mixed_buffer = BytesIO()
        mixed_audio.export(mixed_buffer, format="wav")
        mixed_buffer.seek(0)

        mg.process(
            target=mixed_buffer,
            reference=reference_path,
            results=[mg.pcm24(output_path)]
        )
        logger.info(f"Mastering completed. Final output saved at {output_path}")

    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")
        raise

def mix_tracks(vocals_path, instrumental_path):
    """Mix vocals and instrumental into one track and return the mixed AudioSegment."""
    try:
        vocals = AudioSegment.from_wav(vocals_path)
        instrumental = AudioSegment.from_wav(instrumental_path)

        if len(vocals) < len(instrumental):
            silence = AudioSegment.silent(duration=len(instrumental) - len(vocals))
            vocals = vocals + silence
        elif len(vocals) > len(instrumental):
            vocals = vocals[:len(instrumental)]

        return vocals.overlay(instrumental)
    except Exception as e:
        logger.error(f"Error mixing tracks: {e}")
        return None

def notify_system_api(song_id, stage, action, file_name=None, err_msg=None):
    """Send a status update to the webhook API."""
    try:
        payload = {
            "songID": song_id,
            "stage": stage,
            "action": action,
            "fileName": file_name,
            "errMsg": err_msg
        }

        print(payload)

        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        logger.info(f"Webhook notified successfully: {action} for songID {song_id}.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to notify webhook: {e}")

def lambda_handler(event, context):
    """AWS Lambda handler function."""
    logger.info("Starting Lambda execution...")
    logger.info(f"Event: {json.dumps(event, indent=4)}")

    song_id = 0

    

    try:
        for record in event['Records']:
            body = json.loads(record['body'])
            song_id = body.get("songID")
            file_name = body.get("fileName")
            track_id = body.get("trackID")

            if not track_id:
                raise ValueError("Region or track ID is missing.")

            initialize_s3_paths(track_id)

            if not LAMBDA_STATIC_INSTRUMENTAL_WAV_PATH:
                raise ValueError("LAMBDA_STATIC_INSTRUMENTAL_WAV_PATH is not set.")

            local_final_file = f"/tmp/final_song_{song_id}.wav"
            local_vocals_path = f"/tmp/vocals_{song_id}.wav"
            lambda_vocals_path = f"utau_inference/{file_name}"

            notify_system_api(song_id, "matchering", "start", file_name=None, err_msg=None)


            if wait_for_file(bucket_name, LAMBDA_STATIC_REFERENCE_WAV_PATH, s3_client):
                download_file_from_s3(bucket_name, LAMBDA_STATIC_REFERENCE_WAV_PATH, LOCAL_REFERENCE_PATH, s3_client)
            else:
                raise Exception("Reference file not available within timeout")

            if wait_for_file(bucket_name, LAMBDA_STATIC_INSTRUMENTAL_WAV_PATH, s3_client):
                download_file_from_s3(bucket_name, LAMBDA_STATIC_INSTRUMENTAL_WAV_PATH, LOCAL_INSTRUMENTAL_PATH, s3_client)
            else:
                raise Exception("Instrumental file not available within timeout")

            if wait_for_file(bucket_name, lambda_vocals_path, s3_client):
                download_file_from_s3(bucket_name, lambda_vocals_path, local_vocals_path, s3_client)
            else:
                raise Exception(f"Vocals file not available: {lambda_vocals_path}")

            process_files(local_vocals_path, LOCAL_INSTRUMENTAL_PATH, local_final_file, LOCAL_REFERENCE_PATH)

            upload_file_to_s3(local_final_file, bucket_name, f"matchering/final_song_{song_id}_{track_id}.wav")

            notify_system_api(song_id, "matchering", "end", file_name=f"final_song_{song_id}_{track_id}.wav", err_msg=None)

    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        notify_system_api(song_id, "matchering", "error", None, str(e))

    finally:
        # Clean up temporary files
        for file_path in [LOCAL_VOCALS_PATH, LOCAL_FINAL_FILE, LOCAL_REFERENCE_PATH, LOCAL_INSTRUMENTAL_PATH]:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.info(f"Deleted temporary file: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete {file_path}: {e}")

        logger.info("Lambda execution completed.")

#For local testing
if __name__ == "__main__":
    try:
        print("Loading input.json...")
        with open("./input.json", "r") as f:
            event = json.load(f)
        
        print("Starting local execution...")
        result = lambda_handler(event, None)
        print("\nExecution Result:")
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        print(f"Error during local execution: {e}")