import pandas as pd
import ipywidgets as widgets
from IPython.display import display

def show_excel_like(df, width='1000px', height='400px'):
    """
    Displays a pandas DataFrame in an Excel-like format with scroll bars
    """
    
    # Generate styled HTML with borders and Excel-like formatting
    styled = df.style \
        .set_properties(**{
            'border': '1px solid #d0d0d0',
            'text-align': 'left',
            'background-color': 'white',
            'font-family': 'Arial',
            'font-size': '12px'
        }) \
        .set_table_styles([{
            'selector': 'th',
            'props': [
                ('background-color', '#f0f0f0'),
                ('color', 'black'),
                ('font-weight', 'bold'),
                ('border', '1px solid #d0d0d0'),
                ('position', 'sticky'),
                ('left', '0'),
                ('z-index', '1')
            ]
        }])

    # Use appropriate method based on pandas version
    if hasattr(styled, 'render'):
        styled_html = styled.render()  # For pandas >= 1.3.0
    else:
        styled_html = styled._repr_html_()  # For older pandas versions

    # Create HTML widget with styled table
    html_widget = widgets.HTML(value=styled_html)

    # Create scrollable container
    container = widgets.Box(
        children=[html_widget],
        layout=widgets.Layout(
            overflow='auto',
            width=width,
            height=height,
            border='1px solid #c0c0c0',
            margin='10px'
        )
    )

    display(container)

if __name__ == "__main__":
    text = input("Yell something at a mountain: ")
    print(text)