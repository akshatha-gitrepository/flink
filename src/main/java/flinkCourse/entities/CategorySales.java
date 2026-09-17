package flinkCourse.entities;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CategorySales {

    public String category;
    public Float totalSales;
    public Integer count;
}
