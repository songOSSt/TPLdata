import os
import subprocess
import time
import shutil
import signal
import sys
import re
import glob
import datetime
from function_processor import process_functions_parallel, save_tokens_batch, process_single_file

# Global variables for accessing in signal handler
processing_repo_path = None
processing_original_branch = None

def cleanup_git_repo(repo_path, original_branch):
    """Clean up and restore git repository"""
    if not repo_path or not os.path.exists(os.path.join(repo_path, '.git')):
        return
    
    print(f"\nRestoring git repository {repo_path} to original state...")
    
    try:
        # Save current working directory
        original_dir = os.getcwd()
        
        # Switch to repository directory
        os.chdir(repo_path)
        
        # Clean up lock files
        remove_git_locks(repo_path)
        
        # Try to restore to original branch
        if original_branch:
            try:
                subprocess.run(f"git checkout -f {original_branch}", shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                subprocess.run("git reset --hard", shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                print(f"Successfully restored to original branch: {original_branch}")
            except Exception as e:
                print(f"Error restoring to original branch: {e}")
        
        # Restore original working directory
        os.chdir(original_dir)
    except Exception as e:
        print(f"Error cleaning up git repository: {e}")

def signal_handler(signum, frame):
    """Handle program interrupt signals"""
    print(f"\nReceived interrupt signal {signum}, starting cleanup...")
    
    if processing_repo_path and processing_original_branch:
        cleanup_git_repo(processing_repo_path, processing_original_branch)
    
    print("Cleanup completed, exiting program...")
    sys.exit(1)

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # Termination signal

def remove_git_locks(repo_path):
    """Remove Git lock files"""
    lock_files = [
        os.path.join(repo_path, '.git', 'index.lock'),
        os.path.join(repo_path, '.git', 'HEAD.lock'),
        os.path.join(repo_path, '.git', 'refs', 'heads', '*.lock'),
    ]

    for lock_file in lock_files:
        try:
            files_to_remove = glob.glob(lock_file) if '*' in lock_file else ([lock_file] if os.path.exists(lock_file) else [])
            for f in files_to_remove:
                if os.path.exists(f):
                    os.remove(f)
        except Exception as e:
            print(f"Error removing lock file {lock_file}: {e}")


def clean_workspace(repo_path):
    """Clean git workspace with retry mechanism"""
    try:
        git_with_retry("git reset --hard HEAD", repo_path)
        git_with_retry("git clean -fd", repo_path)
    except Exception as e:
        print(f"Failed to clean workspace: {e}")
        # Try more aggressive cleanup
        try:
            subprocess.run("git reset --hard", shell=True, cwd=repo_path,
                          stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            subprocess.run("git clean -fd", shell=True, cwd=repo_path,
                          stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            remove_git_locks(repo_path)
        except:
            pass


def get_tag_hash(repo_path, tag):
    """Get full hash value of a tag"""
    try:
        return git_with_retry(f"git rev-list -n1 {tag}", repo_path).decode().strip()
    except subprocess.CalledProcessError:
        return None


def git_with_retry(cmd, repo_path, max_retries=3, wait_time=1):
    """Execute git command with retry mechanism"""
    for attempt in range(max_retries):
        try:
            # Clean lock files before each attempt
            remove_git_locks(repo_path)
            
            # Execute git command
            if "rev-list" in cmd or "rev-parse" in cmd:
                # Special handling for hash retrieval commands that may have warnings
                result = subprocess.check_output(cmd, shell=True, stderr=subprocess.PIPE, text=True, cwd=repo_path)
                # If output contains multiple lines, extract the last line as valid hash
                lines = result.strip().split('\n')
                # Filter out warning lines
                hash_lines = [line for line in lines if not line.startswith('warning:')]
                if hash_lines:
                    result = hash_lines[-1].encode()  # Convert back to bytes
                else:
                    raise subprocess.CalledProcessError(1, cmd, "Failed to get valid hash value")
            else:
                # Other commands execute normally
                result = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, cwd=repo_path)
            
            # Check if it's just a Git LFS warning
            if "git-lfs" in result.decode('utf-8', errors='replace') and "was not found on your path" in result.decode('utf-8', errors='replace'):
                # Check if command actually executed successfully
                if "HEAD is now at" in result.decode('utf-8', errors='replace'):
                    # Extract actual result from output, ignore LFS warnings
                    return result
            
            return result
        except subprocess.CalledProcessError as e:
            error_msg = e.output.decode('utf-8', errors='replace') if e.output else str(e)
            
            # Check if it's an ambiguous reference warning when getting hash value
            if "warning: refname" in error_msg and "is ambiguous" in error_msg:
                # Try to extract full hash from error message
                hash_match = re.search(r'([0-9a-f]{40})', error_msg)
                if hash_match:
                    return hash_match.group(1).encode()
            
            # Check if it's a fatal error, some errors won't work even with retry
            fatal_errors = [
                "fatal: not a git repository",
                "fatal: unable to access",
                "fatal: authentication failed",
                "fatal: could not read Username",
                "fatal: repository not found"
            ]
            
            if any(err in error_msg for err in fatal_errors):
                print(f"Git command execution failed (fatal error): {cmd}\nError message: {error_msg}")
                raise
                
            # Check if it's an ambiguous reference error
            if "checkout" in cmd and ("did not match any file" in error_msg or "pathspec" in error_msg):
                # Try to extract hash value part from command
                cmd_parts = cmd.split()
                for part in cmd_parts:
                    if re.match(r'^[0-9a-f]{7,40}$', part):
                        # Try using full hash value query
                        try:
                            full_hash = subprocess.check_output(
                                f"git rev-parse {part}", 
                                shell=True, stderr=subprocess.PIPE, text=True, cwd=repo_path
                            ).strip()
                            # Reconstruct command using full hash
                            new_cmd = cmd.replace(part, full_hash)
                            return git_with_retry(new_cmd, repo_path, max_retries - attempt - 1, wait_time)
                        except:
                            pass
            
            # If this is the last attempt and it failed
            if attempt == max_retries - 1:
                print(f"Git command failed after multiple retries: {cmd}\nError message: {error_msg}")
                
                # Try to reset repository state
                try:
                    # Try to abort current operation
                    subprocess.run("git reset --hard", shell=True, cwd=repo_path, 
                                  stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                    # Clean index
                    subprocess.run("git clean -fd", shell=True, cwd=repo_path,
                                  stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                    # Clean lock files
                    remove_git_locks(repo_path)
                except Exception as reset_error:
                    print(f"Failed to reset repository state: {reset_error}")
                
                raise
            
            # Not the last attempt, log error and wait for retry
            if attempt < max_retries - 1:
                print(f"Git command failed, waiting to retry: {cmd}\nError message: {error_msg}")
                time.sleep(wait_time * (attempt + 1))  # Incremental wait time


def sort_tags_by_date(repo_path, tags, repo_name=None, result_path=None):
    """Sort tags by commit date

    Prefer using locally saved tag date information, fall back to git command if not found
    """
    # If repository name and result path are provided, try to load date information from local file
    if repo_name and result_path:
        tag_dates = load_tag_dates(repo_name, result_path)
        if tag_dates:
            # Filter out the tags we need
            filtered_tag_dates = [(tag, date) for tag, date in tag_dates if tag in tags]
            if filtered_tag_dates:
                # Sort by date
                filtered_tag_dates.sort(key=lambda x: x[1])
                return [tag for tag, _ in filtered_tag_dates]
            # If no matching tags found, fall back to git command

    # Fall back to using git command
    print("Using git command to get tag date information")
    tag_dates = []
    for tag in tags:
        try:
            # Get commit date of the tag
            cmd = f'git log -1 --format=%at {tag}'
            date_str = git_with_retry(cmd, repo_path).decode().strip()
            tag_dates.append((tag, int(date_str)))
        except Exception:
            # If getting date fails, use 0 as date (placed first)
            tag_dates.append((tag, 0))

    # Sort by date
    tag_dates.sort(key=lambda x: x[1])
    return [tag for tag, _ in tag_dates]


def load_tag_dates(repo_name, result_path):
    """Load tag date information from local file

    Returns format: [(tag_name, timestamp), ...]
    """
    from config import Config
    config = Config()

    # Get tag date information path from config
    tag_date_path = getattr(config, 'tagDatePath', None)
    if not tag_date_path:
        # If path not specified in config, use default path
        base_path = os.path.dirname(result_path)
        tag_date_path = os.path.join(base_path, "repo_date")

    tag_date_file = os.path.join(tag_date_path, repo_name)

    if not os.path.exists(tag_date_file):
        print(f"Tag date information file does not exist: {tag_date_file}")
        return None

    try:
        tag_dates = []
        with open(tag_date_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line == "None" or line.startswith("Error:"):
                    # Repository has no tags or error occurred
                    return []

                # Parse date and tag information
                # Format example: "2021-10-15 (tag: v1.0.0, sanitized: v1.0.0)"
                if "(" in line and ")" in line:
                    date_part = line.split(" (")[0].strip()
                    info_part = line.split(" (")[1].split(")")[0]

                    # Extract original tag name
                    tag_match = re.search(r'tag: (.*?),', info_part)
                    if tag_match:
                        tag = tag_match.group(1)

                        # Convert date to timestamp
                        if date_part != "None":
                            try:
                                # Convert YYYY-MM-DD to timestamp
                                dt = datetime.datetime.strptime(date_part, "%Y-%m-%d")
                                timestamp = int(dt.timestamp())
                                tag_dates.append((tag, timestamp))
                            except Exception as e:
                                print(f"Failed to parse date: {date_part} - {e}")
                                # Use 0 as date (placed first)
                                tag_dates.append((tag, 0))
                        else:
                            # Date is None, use 0 as timestamp
                            tag_dates.append((tag, 0))

        return tag_dates
    except Exception as e:
        print(f"Failed to load tag date information: {e}")
        return None


# Add sanitize_tag_name parameter to function signature
def process_repo_tags_incremental(repo_path, repo_name, result_path, tags_to_process=None, sanitize_tag_name=None):
    """Incrementally process repository tags"""
    try:
        # Global variables for signal handling
        global processing_repo_path, processing_original_branch
        
        # Save current working directory
        original_dir = os.getcwd()

        # Switch to repository directory
        os.chdir(repo_path)
        print(f"\nStarting incremental processing of repository: {repo_name}")

        try:
            # Perform a comprehensive cleanup first to ensure starting from clean state
            print("Executing initialization cleanup...")
            try:
                # Abort any possible merge, rebase, etc. operations
                subprocess.run("git merge --abort", shell=True, cwd=repo_path, 
                              stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                subprocess.run("git rebase --abort", shell=True, cwd=repo_path,
                              stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                # Reset and clean workspace
                subprocess.run("git reset --hard", shell=True, cwd=repo_path, 
                              stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                subprocess.run("git clean -fd", shell=True, cwd=repo_path,
                              stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            except Exception as e:
                print(f"Error during initialization cleanup: {e}")
            
            # Clean all lock files
            remove_git_locks(repo_path)

            # Check if it's a git repository
            if not os.path.exists(os.path.join(repo_path, '.git')):
                print(f"Error: {repo_path} is not a valid Git repository")
                return False

            # Get all tags or use specified tag set
            try:
                if tags_to_process is None:
                    tagResult = git_with_retry(
                        "git tag --list",
                        repo_path
                    ).decode()
                    tags = [tag.strip() for tag in tagResult.split('\n') if tag.strip()]
                else:
                    tags = list(tags_to_process)
            except subprocess.CalledProcessError as e:
                print(f"Failed to get tag list: {e}")
                return False

            # Save current branch
            try:
                current_branch = git_with_retry(
                    "git rev-parse --abbrev-ref HEAD",
                    repo_path
                ).decode().strip()
                
                # Set global variables for signal handling
                processing_repo_path = repo_path
                processing_original_branch = current_branch
                
            except:
                current_branch = "HEAD"
                processing_original_branch = "HEAD"

            try:
                if not tags:
                    # No tags, process main branch
                    print(f"Repository {repo_name} has no tags to process, processing main branch")
                    tokens_list = process_functions_parallel(repo_path)
                    if tokens_list:
                        save_tokens_batch(tokens_list, repo_name, None, result_path)
                        print(f"Successfully processed main branch, got {len(tokens_list)} functions")
                else:
                    # Has tags, process in chronological order
                    print(f"Repository {repo_name} starting to process {len(tags)} tags")

                    # Sort tags by time, pass repository name to use local tag date information
                    sorted_tags = sort_tags_by_date(repo_path, tags, repo_name, result_path)

                    # Record previously processed tag and hash value
                    prev_tag = None
                    prev_tag_hash = None

                    # Record processed tags for rollback mechanism
                    processed_tags = []

                    for tag in sorted_tags:
                        if not tag.strip():
                            continue

                        print(f"Processing tag: {tag}")
                        
                        # Check if result file already exists
                        sanitized_tag = _get_sanitized_tag_name(tag, sanitize_tag_name)
                        result_file = os.path.join(result_path, repo_name, f"fuzzy_{sanitized_tag}.tokens")
                        if os.path.exists(result_file) and os.path.getsize(result_file) > 0:
                            print(f"Result file for tag {tag} already exists and is not empty, skipping processing")
                            # Update previous tag information
                            tag_hash = get_tag_hash(repo_path, tag)
                            if tag_hash:
                                prev_tag = tag
                                prev_tag_hash = tag_hash
                                if tag not in processed_tags:
                                    processed_tags.append(tag)
                            else:
                                print(f"Warning: Failed to get hash value for skipped tag {tag}")
                            continue

                        # Clean workspace to ensure starting from clean state for each tag
                        clean_workspace(repo_path)

                        # Get full hash value of the tag and checkout
                        tag_hash = get_tag_hash(repo_path, tag)
                        if not tag_hash:
                            print(f"Skipping tag {tag}: Unable to get tag hash")
                            continue

                        try:
                            git_with_retry(f"git checkout -f {tag_hash}", repo_path)
                        except subprocess.CalledProcessError:
                            print(f"Skipping tag {tag}: Unable to switch to tag")
                            continue
                        
                        # Add to processed list after successfully checking out tag
                        processed_tags.append(tag)

                        # If there's a previous tag, check differences between the two tags
                        if prev_tag and prev_tag_hash:
                            # Get changed files
                            changed_files = get_changed_files(repo_path, prev_tag_hash, tag_hash)

                            if not changed_files:
                                # If no file changes, try to copy previous tag's results
                                print(f"Tag {tag} has no file changes compared to {prev_tag}, trying to copy results")
                                copy_success = copy_previous_tag_results(repo_name, prev_tag, tag, result_path,
                                                                         sanitize_tag_name)
                                
                                # Modified strategy: If copy fails, perform full processing directly instead of trying other tags
                                if not copy_success:
                                    print(f"Warning: Unable to copy previous tag {prev_tag} results, performing full processing")
                                    all_tokens_list = process_functions_parallel(repo_path)
                                    if all_tokens_list:
                                        save_tokens_batch(all_tokens_list, repo_name, tag, result_path)
                                        print(f"Successfully fully processed tag {tag}, got {len(all_tokens_list)} functions")
                                    
                                    # Update previous tag information
                                    prev_tag = tag
                                    prev_tag_hash = tag_hash
                                    continue  # Continue processing next tag
                                else:
                                    # Copy successful, continue processing next tag
                                    prev_tag = tag
                                    prev_tag_hash = tag_hash
                                    continue

                            print(f"Tag {tag} has {len(changed_files)} file changes compared to {prev_tag}")

                            # Only process changed files
                            tokens_list = process_changed_files(repo_path, changed_files)

                            # Get previous tag's processing results
                            prev_tokens = load_previous_tag_tokens(repo_name, prev_tag, result_path, sanitize_tag_name)

                            # Modified strategy: If unable to load previous tag's results, perform full processing directly
                            if not prev_tokens:
                                print(f"Warning: Unable to load previous tag {prev_tag} processing results, performing full processing")
                                tokens_list = process_functions_parallel(repo_path)
                            else:
                                # Merge results
                                # Remove functions from changed files from previous tag's results
                                changed_file_paths = set(changed_files)  # Already relative paths
                                
                                # Ensure only keep tokens from unchanged files
                                unchanged_tokens = []
                                for tokens, file_path in prev_tokens:
                                    if file_path not in changed_file_paths:
                                        unchanged_tokens.append((tokens, file_path))

                                # Merge unchanged functions and newly processed functions
                                tokens_list.extend(unchanged_tokens)
                        else:
                            # First tag, process all files
                            tokens_list = process_functions_parallel(repo_path)

                        # Save results
                        if tokens_list:
                            save_tokens_batch(tokens_list, repo_name, tag, result_path)
                            print(f"Successfully processed tag {tag}, got {len(tokens_list)} functions")

                        # Update previous tag information
                        prev_tag = tag
                        prev_tag_hash = tag_hash

                    print(f"All specified tags for repository {repo_name} processing completed")
                    
                    # After processing all tags, ensure restore to original branch
                    print(f"Restoring to original branch: {current_branch}")
                    git_with_retry(f"git checkout -f {current_branch}", repo_path)

            finally:
                # Try to restore to original branch regardless of processing success
                cleanup_git_repo(repo_path, current_branch)
                
                # Reset global variables
                processing_repo_path = None
                processing_original_branch = None

        finally:
            # Restore original working directory
            os.chdir(original_dir)

    except Exception as e:
        print(f"Unknown error processing repository {repo_name}: {e}")
        
        # Ensure cleanup of git repository even on error
        cleanup_git_repo(repo_path, processing_original_branch)
        processing_repo_path = None
        processing_original_branch = None
        
        return False

    return True


def get_changed_files(repo_path, prev_tag, current_tag):
    """Get list of files that changed between two tags"""
    try:
        cmd = f'git diff --name-only {prev_tag} {current_tag}'
        result = git_with_retry(cmd, repo_path).decode().strip()
        if result:
            # Only return C/C++ source files
            return [f for f in result.split('\n') if f.endswith(('.c', '.cc', '.cpp'))]
        return []
    except Exception as e:
        print(f"Failed to get changed files: {e}")
        return []


def _get_sanitized_tag_name(tag, sanitize_tag_name):
    """Get sanitized tag name if sanitize function provided, otherwise return original"""
    return sanitize_tag_name(tag) if sanitize_tag_name else tag


def copy_previous_tag_results(repo_name, prev_tag, current_tag, result_path, sanitize_tag_name):
    """Copy previous tag's processing results to current tag"""
    prev_sanitized = _get_sanitized_tag_name(prev_tag, sanitize_tag_name)
    current_sanitized = _get_sanitized_tag_name(current_tag, sanitize_tag_name)
    prev_file = os.path.join(result_path, repo_name, f"fuzzy_{prev_sanitized}.tokens")
    current_file = os.path.join(result_path, repo_name, f"fuzzy_{current_sanitized}.tokens")

    # Ensure target directory exists
    os.makedirs(os.path.dirname(current_file), exist_ok=True)

    # Simplified logic: only check if previous tag's result file exists, don't try to find other tags
    if not os.path.exists(prev_file):
        print(f"Warning: Cannot find processing result file for previous tag {prev_tag}")
        return False

    # Copy file
    try:
        shutil.copy2(prev_file, current_file)
        print(f"Successfully copied {prev_tag} results to {current_tag}")
        return True
    except Exception as e:
        print(f"Failed to copy file: {e}")
        return False


def _parse_token_line(line):
    """Parse a single token line from file"""
    line = line.strip()
    if not line:
        return None
    if '|' in line:
        tokens, file_path = line.split('|', 1)
        file_path = file_path.replace('\\', '/').strip()
        return (tokens.replace(',', ' '), file_path)
    else:
        # Compatible with old format, no path information
        return (line.replace(',', ' '), "unknown.c")


def load_previous_tag_tokens(repo_name, prev_tag, result_path, sanitize_tag_name):
    """Load previous tag's processing results"""
    prev_sanitized = _get_sanitized_tag_name(prev_tag, sanitize_tag_name)
    prev_file = os.path.join(result_path, repo_name, f"fuzzy_{prev_sanitized}.tokens")

    if not os.path.exists(prev_file):
        print(f"Warning: Cannot find processing result file for previous tag {prev_tag}")
        return []

    # Read previous tag's tokens, try utf-8 first, then latin-1
    tokens_list = []
    for encoding in ('utf-8', 'latin-1'):
        try:
            with open(prev_file, 'r', encoding=encoding) as f:
                for line in f:
                    parsed = _parse_token_line(line)
                    if parsed:
                        tokens_list.append(parsed)
            if encoding == 'latin-1':
                print(f"Successfully read using latin-1 encoding")
            break
        except Exception as e:
            if encoding == 'utf-8':
                print(f"Failed to read previous tag's tokens: {e}")
            else:
                print(f"Failed to read with alternative encoding: {e}")

    # Check if unknown.c exists
    unknown_count = sum(1 for _, path in tokens_list if path == "unknown.c")
    if unknown_count > 0:
        print(f"Warning: {unknown_count} tokens without path information (marked as unknown.c)")

    return tokens_list


def process_changed_files(repo_path, changed_files):
    """Only process changed files, ensure returned tokens list contains relative path information for files"""
    tokens_list = []
    for file in changed_files:
        file_path = os.path.join(repo_path, file)
        if os.path.exists(file_path):
            # Get tokens for this file
            file_tokens = process_single_file(file_path)
            
            # Ensure each token has corresponding relative path information
            for token in file_tokens:
                # Check token format, ensure compatibility with old format
                if isinstance(token, tuple) and len(token) == 2:
                    # Already has path information, but might be absolute path, convert to relative path
                    token_str, token_path = token
                    # If absolute path, convert to relative path
                    if os.path.isabs(token_path) and token_path.startswith(repo_path):
                        rel_path = os.path.relpath(token_path, repo_path)
                        tokens_list.append((token_str, rel_path))
                    else:
                        # Already relative path or cannot determine, use directly
                        tokens_list.append(token)
                else:
                    # Old format, no path information, add relative path
                    tokens_list.append((token, file))
    
    return tokens_list