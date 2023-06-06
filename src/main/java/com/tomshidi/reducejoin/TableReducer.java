package com.tomshidi.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author TomShiDi
 * @since 2023/6/5 21:58
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        List<TableBean> orderBeanList = new ArrayList<>();
        TableBean pdBean = new TableBean();

        for (TableBean tableBean : values) {
            if ("order".equals(tableBean.getFrom())) {
                TableBean t = new TableBean();
                try {
                    BeanUtils.copyProperties(t, tableBean);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
                orderBeanList.add(t);
            } else if ("pd".equals(tableBean.getFrom())) {
                try {
                    BeanUtils.copyProperties(pdBean, tableBean);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        for (TableBean tableBean : orderBeanList) {
            tableBean.setPname(pdBean.getPname());
            context.write(tableBean, NullWritable.get());
        }
    }
}
