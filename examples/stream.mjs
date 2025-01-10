import { Readable } from "stream";
import { RustStream } from "../index.js";


// TODO: Could have a small JS wrapper that wrap .stream() from QueryResult so that we have a Node.js native stream
function createReadableStream(maxChunks) {
  const rustStream = new RustStream(maxChunks);

  return new Readable({
    objectMode: true,
    read(size) {
      try {
        const chunk = rustStream.read();
        if (chunk === null) {
          this.push(null); // End of stream
        } else {
          this.push(Buffer.from(chunk)); // Convert napi Buffer to Node.js Buffer
        }
      } catch (err) {
        this.destroy(err);
      }
    },
  });
}

// Example usage
const readable = createReadableStream(10);
readable.on("data", (chunk) => {
  console.log("Received chunk:", chunk.toString());
});
readable.on("end", () => {
  console.log("Stream ended.");
});
