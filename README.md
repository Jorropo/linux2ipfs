# linux2ipfs

Linux to IPFS is a project that aims to pipeline streaming a linux repository to [estuary.tech](https://estuary.tech/).

# Requirement

- Linux >= v5.3
- A reflinking-able FS (btrfs, XFS (optional) or ZFS).
- Modifications times enabled on your FS.
- A reasonably updated time clock.
- A 64bits kernel.

# Performance

This run has been recorded on the `9ad2437383dfe9e1c8851e4d4f35f0bdd8580027` (Feb 14 2022) commit.

It compares to [ipld/go-car](https://github.com/ipld/go-car) `v2.1.1`.

The test was adding a ~32Gig files of `/dev/urandom` data.

It was done on an RAID1 btrfs of 2 sata SSDs.

## linux2ipfs

```console
$ time linux2ipfs-test -driver car -concurrent-chunkers 32 f
bafybeigpau6jiycwadkj74svqbpaibqblmttkjzb53avaxohmahnfojzgm
updated

real	1m25.407s
user	3m18.423s
sys	0m46.643s
```
For info that is faster than the write speed of my disks.

This is achieved thx to reflinking, basically instead of copying the data to the car. It "reflinks" it. That means creating a Copy-On-Write copy, so they share the same on disk data, however in case of any modification a copy on write of that precise part of the file is created.

```console
$ btrfs fi du *
     Total   Exclusive  Set shared  Filename
  31.91GiB       0.00B    31.91GiB  f
  31.97GiB    65.37MiB    31.91GiB  out.1.car
```
The overhead is only `65.37MiB` the rest `31.91GiB` data is shared between the original `f` file and the car.

## go-car

```console
$ time car c -o car.car f

real	14m59.512s
user	7m47.838s
sys	1m20.531s
```
As you can see, **linux2ipfs is 10 times faster !**

That easy to explain why, most of it is because `go-car` actually copies the file (that the main point however there are other things):
```console
$ btrfs fi du *
     Total   Exclusive  Set shared  Filename
  31.92GiB    31.92GiB       0.00B  car.car
  31.91GiB       0.00B    31.91GiB  f
```
