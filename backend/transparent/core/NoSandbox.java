package transparent.core;

import java.io.IOException;

public class NoSandbox implements Sandbox
{
	@Override
	public Process run(Module module)
	{
		String command = module.getPath();
		try {
			return Runtime.getRuntime().exec(command);
		} catch (IOException e) {
			System.err.println("NoSandbox.run ERROR:"
					+ " Error executing command '" + command + "'.");
			return null;
		}
	}
}
