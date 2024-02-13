# Setting up Your Dev Environment

We are leaning into using Nix for the project. In theory, this should make it
easier to get set up developing on the project. Try the following steps if you
like.

## Install Nix

If you don't already have it, [install Nix](https://nixos.org/download):
```bash
sh <(curl -L https://nixos.org/nix/install) --daemon
```

## Install Other Critical Software

### Git

Get `git` if you don't already have it. I've been using `nix profile` for this,
though I hear good things about
[`home-manager`](https://github.com/nix-community/home-manager). If you're
using `nix profile`:
```bash
nix profile install 'nixpkgs#git'
```

### Direnv

Our repository is set up to be used with [`direnv`](https://direnv.net/). If
you want to give it try, you should install `direnv` and `nix-direnv`:
```bash
nix profile install 'nixpkgs#direnv'
nix profile install 'nixpkgs#nix-direnv'
```

Next, [set up `direnv`](https://direnv.net/docs/hook.html).

You now need to set up `nix-direnv`:
```bash
mkdir -p ~/.config/direnv
echo 'source $HOME/.nix-profile/share/nix-direnv/direnvrc' >> ~/.config/direnv/direnvrc
```

## Try it Out

Get the repository:
```bash
git clone https://github.com/maelstrom-software/maelstrom.git
```

When you first cd into the directory, you should get a `direnv` error. You need to fix that with `direnv allow`:
```bash
cd maelstrom
direnv allow
```

You should now have everything you need to develop on the project when you're in this directory:
```bash
rustc --version
which rustc
cargo build
```
