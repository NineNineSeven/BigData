package mp.hadoop.project.utils;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PersonLink {
    private String link_target_name;
    private Double relation_ratio;
    public PersonLink(String str) {
        String[] split = str.split(",");
        link_target_name = split[0];
        relation_ratio = Double.parseDouble(split[1]);
    }
    public String toString() {
        return String.format("%s,%.4f|", link_target_name, relation_ratio);
    }
}
