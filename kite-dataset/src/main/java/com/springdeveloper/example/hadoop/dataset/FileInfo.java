package com.springdeveloper.example.hadoop.dataset;

/**
 */
public class FileInfo {
	private String name;
	private String path;
	private String pkey;
	private long size;
	private long modified;

	public FileInfo(String name, String path, String pkey, long size, long modified) {
		this.name = name;
		this.path = path;
		this.pkey = pkey;
		this.size = size;
		this.modified = modified;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getPkey() {
		return pkey;
	}

	public void setPkey(String pkey) {
		this.pkey = pkey;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public long getModified() {
		return modified;
	}

	public void setModified(long modified) {
		this.modified = modified;
	}
}
