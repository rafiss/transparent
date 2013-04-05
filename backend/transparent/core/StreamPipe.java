package transparent.core;

import java.io.IOException;
import java.io.OutputStream;

public class StreamPipe implements Runnable
{
	private static final int BUFFER_SIZE = 4096;
	
	private InterruptableInputStream in;
	private OutputStream out;
	private boolean alive;
	
	public StreamPipe(InterruptableInputStream in, OutputStream out) {
		this.in = in;
		this.out = out;
		this.alive = true;
	}
	
	public void stop() {
		this.alive = false;
	}

	@Override
	public void run() {
		byte[] buffer = new byte[BUFFER_SIZE];
		do {
			try {
				int read = in.read(buffer);
				out.write(buffer, 0, read);
			} catch (IOException e) {
				break;
			}
		} while (alive);
	}
}
