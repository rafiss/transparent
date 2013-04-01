package transparent.core;

import java.io.InputStream;
import java.io.OutputStream;

public class StreamPipe implements Runnable
{
	private static final int BUFFER_SIZE = 4096;
	private static final int SLEEP_DURATION = 400;
	
	private InputStream in;
	private OutputStream out;
	private boolean alive;
	
	public StreamPipe(InputStream in, OutputStream out) {
		this.in = in;
		this.out = out;
		this.alive = true;
	}
	
	public void setAlive(boolean alive) {
		this.alive = alive;
	}

	@Override
	public void run() {
		byte[] buffer = new byte[BUFFER_SIZE];
		do {
			try {
				Thread.sleep(SLEEP_DURATION);
			} catch (InterruptedException e) { }

			try {
				if (in.available() > 0) {
					int read = in.read(buffer);
					out.write(buffer, 0, read);
				}
			} catch (Exception e) {
				break;
			}
		} while (alive);
	}
}
