package transparent.core;

import java.io.IOException;

public class InterruptedStreamException extends IOException
{
	private static final long serialVersionUID = -17815229151733528L;

	public InterruptedStreamException() {
		super();
	}

	public InterruptedStreamException(String message) {
		super(message);
	}

	public InterruptedStreamException(String message, Throwable cause) {
		super(message, cause);
	}

	public InterruptedStreamException(Throwable cause) {
		super(cause);
	}
}
