
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.yarn.webapp.view.HtmlPage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PageRankIntermediate implements Writable {
    public List<String> links;
    public Double value;

    public PageRankIntermediate(List<String> links, Double value) {
        this.links = links;
        this.value = value;
    }

    public PageRankIntermediate() {
        this.links = new ArrayList<String>();
        this.value = 0.0;
    }

    public void write (DataOutput out) throws IOException {
        out.writeUTF(links.toString());
        out.writeDouble(value);
    }

    public void readFields(DataInput in) throws IOException {
        String[] linksArray = in.readUTF().split(",");
        links = new ArrayList<String>();
        for (String link : linksArray) {
            links.add(link);
        }
        value = in.readDouble();
    }

    public void writeFields(DataOutput out) throws IOException {
        StringBuilder result = new StringBuilder();
        for (String link : links) {
            result.append(link).append(" ");
        }
        result.append(value);
        out.writeUTF(result.toString());
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (String link : links) {
            result.append(link).append(" ");
        }
        result.append(value);
        return result.toString();
    }
}
