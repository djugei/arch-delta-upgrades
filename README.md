# arch-delta
A project to bring back delta upgrades to Arch Linux.

By calculating, transferring and applying only the differences between package versions,
arch-delta saves a lot of bandwidth compared to transferring the full package.
The tradeoff is increased compute.

## Project Structure
See the respective readme-files for more information.
### [deltaserver](./server)
Generates and serves deltas, you probably will not need to run this.

### [deltaclient](./client)
Requests, downloads, and applies deltas. Run this.

### [async_file_cache](./async_file_cache)
An async enabled cache for things that generate files.
Used for downloading packets and generating deltas in this project,
may be useful for other projects too.
