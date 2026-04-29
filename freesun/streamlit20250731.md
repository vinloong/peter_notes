streamlit

以下是 Streamlit 主要组件的分类整理表格，包含核心功能及典型应用场景：

| **组件类型**   | **组件名称/函数**              | **主要功能**                 | **使用示例**                                          |
| -------------- | ------------------------------ | ---------------------------- | ----------------------------------------------------- |
| **文本展示**   | `st.title()`                   | 添加大标题                   | `st.title("数据分析报告")`                            |
|                | `st.header()`                  | 二级标题                     | `st.header("用户行为统计")`                           |
|                | `st.subheader()`               | 三级标题                     | `st.subheader("2023年Q3数据")`                        |
|                | `st.text()`                    | 固定格式文本                 | `st.text("原始数据格式要求...")`                      |
|                | `st.markdown()`                | 渲染Markdown文本             | `st.markdown("**加粗文字**")`                         |
|                | `st.code()`                    | 显示代码块                   | `st.code("print('Hello')", language='python')`        |
| **数据输入**   | `st.text_input()`              | 文本输入框                   | `name = st.text_input("请输入姓名")`                  |
|                | `st.number_input()`            | 数字输入框                   | `age = st.number_input("年龄", min_value=0)`          |
|                | `st.selectbox()`               | 下拉选择框                   | `option = st.selectbox("城市", ["北京", "上海"])`     |
|                | `st.multiselect()`             | 多选下拉框                   | `colors = st.multiselect("颜色", ["红", "绿", "蓝"])` |
|                | `st.slider()`                  | 滑动条输入                   | `value = st.slider("阈值", 0.0, 1.0, 0.5)`            |
|                | `st.file_uploader()`           | 文件上传                     | `uploaded_file = st.file_uploader("上传CSV")`         |
| **数据展示**   | `st.dataframe()`               | 交互式DataFrame展示          | `st.dataframe(df)`                                    |
|                | `st.table()`                   | 静态表格展示                 | `st.table(df.head())`                                 |
|                | `st.json()`                    | 格式化JSON数据               | `st.json({"key": "value"})`                           |
|                | `st.metric()`                  | 指标卡展示                   | `st.metric("增长率", "5.2%", delta="+1.2%")`          |
| **可视化**     | `st.line_chart()`              | 折线图                       | `st.line_chart(df[['A', 'B']])`                       |
|                | `st.bar_chart()`               | 柱状图                       | `st.bar_chart(df['sales'])`                           |
|                | `st.area_chart()`              | 面积图                       | `st.area_chart(df[['X', 'Y']])`                       |
|                | `st.pyplot()`                  | 显示Matplotlib图形           | `st.pyplot(fig)`                                      |
|                | `st.altair_chart()`            | 渲染Altair图表               | `st.altair_chart(alt.Chart(df).mark_bar())`           |
| **布局控制**   | `st.columns()`                 | 分列布局                     | `col1, col2 = st.columns(2)`                          |
|                | `st.expander()`                | 可折叠内容区                 | `with st.expander("详情"): st.write("更多内容")`      |
|                | `st.tabs()`                    | 标签页布局                   | `tab1, tab2 = st.tabs(["图表", "数据"])`              |
| **状态与进度** | `st.progress()`                | 进度条                       | `st.progress(75)`                                     |
|                | `st.spinner()`                 | 加载等待动画                 | `with st.spinner("处理中..."): time.sleep(2)`         |
| **系统交互**   | `st.button()`                  | 触发动作的按钮               | `if st.button("运行"): process_data()`                |
|                | `st.download_button()`         | 文件下载按钮                 | `st.download_button("下载", data, "file.txt")`        |
|                | `st.checkbox()`                | 复选框                       | `if st.checkbox("显示图表"): show_chart()`            |
| **多媒体**     | `st.image()`                   | 显示图片                     | `st.image("photo.jpg", caption="示例")`               |
|                | `st.audio()`                   | 播放音频                     | `st.audio("sound.mp3")`                               |
|                | `st.video()`                   | 播放视频                     | `st.video("demo.mp4")`                                |
| **高级功能**   | `st.cache_data`                | 缓存函数返回的数据           | `@st.cache_data def load_data(): ...`                 |
|                | `st.session_state`             | 跨组件状态管理               | `st.session_state['key'] = value`                     |
|                | `st.experimental_connection()` | 数据库/API连接工具（实验性） | `conn = st.experimental_connection("mysql")`          |





