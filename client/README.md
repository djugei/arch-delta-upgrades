# deltaclient
Requests, downloads, and applies deltas for Arch Linux packages.

Deltas are generated on demand,
  so you get a speedup even if you don't update for a while.
The first time a specific delta is requested might take some time,
  up to minutes,
  this is normal and expected.

No additional signing infrastructure is required.
The official Arch Linux packages are reconstructed bit by bit
  so the regular archlinx-keyring can be utilized.
Check [rootless](#rootless) for even less required trust.

## Installation
Either install deltaclient from the AUR,
  using your favourite AUR-helper,
  or use cargo.

```bash
git clone https://github.com/djugei/arch-delta-upgrades/
cd arch-delta-upgrades/client
cargo run -- upgrade <DELTASERVER_URL>
```

The deltaclient will then proceed to sync databases and upgrade packages using deltas.

## Usage Tips
Never run ```bash pacman -Scc```.
That would delete all cached packages,
making delta upgrades impossible.
You can absolutely run ```bash pacman -Sc``` to delete old packages though.

Check ```bash deltaclient stats``` to see how well its working for you.

If you find some package to take too long or not save enough bandwidth
  you can add them to the --blacklist

## Rootless
Since reading existing packages and downloading new ones from the internet
  is not a privileged action it can be done as a regular user.
Check ```bash deltaclient download --help``` for more instructions.

Database sync using deltas is not yet supported in a rootless manner.
Once it is a systemd unit will be provided to check for updates and prepare packages in the background.
