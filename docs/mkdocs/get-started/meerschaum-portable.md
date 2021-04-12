# Installing Meerschaum Portable
Sometimes, it's not worth the hassle setting up a complete Python environment. If you're using a non-administrative account or just want to test out Meerschaum, Meerschaum Portable might be right for you.

Meerschaum Portable is a self-contained Meerschaum installation, running entirely within one directory. Download and extract the archive, and you can have Meerschaum running in seconds.

!!! bug "Compatability Notice"
    Although Meerschaum Portable is extensively tested, you may still encounter problems, such as permissions issues on Mac OS. In case you cannot successfully extract Meerschaum Portable, consider following the directions for a normal install found on the [Getting Started](/get-started) page.

## Installing Meerschaum Portable
Installing Meerschaum Portable is pretty straightforward: download and extract the archive file, then execute `mrsm`. Follow the steps below for more information.

### Download the Archive

Download the appropriate archive for your operating system. For convenience, I recommend installing the Full version, though note that the extracted folder can grow to >1 GB in size!

If you don't need all of Meerschaum's functionality and would rather save a bit of disk space, download the Minimal version. You can always install dependencies later with `mrsm upgrade packages`.

| Windows 10 x64                                               | Linux x64                                                    | MacOS                                                        |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| [Windows Full](https://meerschaum.io/files/mrsm-full-windows.zip) | [Linux Full](https://meerschaum.io/files/mrsm-full-linux.tar.gz) | [MacOS Full](https://meerschaum.io/files/mrsm-full-macos.tar.gz) |
| [Windows Minimal](https://meerschaum.io/files/mrsm-minimal-windows.zip) | [Linux Minimal](https://meerschaum.io/files/mrsm-minimal-linux.tar.gz) | [MacOS Minimal](https://meerschaum.io/files/mrsm-minimal-macos.tar.gz) |

### Extract the Archive

After downloading the archive, move it to your desired location and extract its contents:

=== "Windows"
    Right click the zip file and choose Extract All. Once it's finished extracting, open the folder and double click `setup.bat`. Allow permissions if prompted.
    
=== "Linux"
    Extract the `tar` archive:
    ```
    tar -xvf mrsm-full.tar.gz
    ```

=== "MacOS"
    Double-click the archive to open the Archive Utility, or use `tar` (see Linux instructions).
    
### Run the Script
Inside the extracted folder, there is an executable called `mrsm` (`mrsm.bat` on Windows). Run this script by double-clicking (Windows and MacOS) or via a terminal window. The first time launching may take a bit, so be patient!

### Upgrading Meerschaum Portable
Oftentimes, Meerschaum Portable is a few versions behind the latest official release. To upgrade to the latest release, run the `upgrade meerschaum` command from within `mrsm`:

```bash
upgrade meerschaum
```

## Resetting Meerschaum Portable
To return Meerschaum Portable to its "factory" state, delete the folder called `root` inside the extracted directory. The `root` folder contains Meerschaum data and configuration files, so make sure you back up your data before deleting!

## Uninstalling Meerschaum Portable
On Windows, run the included `uninstall.bat` script to uninstall Meerschaum. On Linux and MacOS, simply delete the extracted directory:
```bash
rm -rf mrsm/
```

!!! warning "Keep Meerschaum Portable in its own folder."
    On Windows, the script `uninstall.bat` deletes the parent folder of `mrsm.bat`. Therefore **do not** keep the contents of the `.zip` folder by themselves on your desktop, or `uninstall.bat` will delete your `Desktop` folder!