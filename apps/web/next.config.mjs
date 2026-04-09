import path from "node:path";
import { fileURLToPath } from "node:url";

const currentDirectory = path.dirname(fileURLToPath(import.meta.url));

const nextConfig = {
  turbopack: {
    root: currentDirectory,
  },
};

export default nextConfig;
