package com.htsc.app.func;

import com.htsc.utils.SplitWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String keyWord) {

        List<String> words = SplitWordUtil.splitKeyWord(keyWord);

        for (String word : words) {
            collect(Row.of(word));
        }

    }

}
