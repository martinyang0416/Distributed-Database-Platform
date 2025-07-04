import os
import ast

def extract_function_info(filepath):
    """
    Extracts function headers and docstrings from a Python file.

    Args:
        filepath (str): The path to the Python file.

    Returns:
        list: A list of tuples, where each tuple contains the function name,
              header string, and docstring. Returns an empty list if parsing fails
              or no functions are found.
    """
    functions_info = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            source_code = f.read()
        tree = ast.parse(source_code)

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                # Extract function name
                function_name = node.name

                # Construct function header
                header_parts = []

                # Handle positional-only arguments (Python 3.8+)
                if hasattr(node.args, 'posonlyargs'):
                    posonly_args = [arg.arg + (f": {ast.unparse(arg.annotation)}" if arg.annotation else "") for arg in node.args.posonlyargs]
                    header_parts.extend(posonly_args)
                    if posonly_args:
                         header_parts.append('/') # Separator for positional-only arguments

                # Handle positional and keyword arguments
                def format_arg_with_default(arg, default):
                    arg_str = arg.arg
                    if arg.annotation:
                        arg_str += f": {ast.unparse(arg.annotation)}"
                    if default:
                        arg_str += f"={ast.unparse(default)}"
                    return arg_str

                num_defaults = len(node.args.defaults)
                args_with_defaults = node.args.args[len(node.args.args) - num_defaults:]
                args_without_defaults = node.args.args[:len(node.args.args) - num_defaults]

                header_parts.extend([format_arg_with_default(arg, None) for arg in args_without_defaults])
                header_parts.extend([format_arg_with_default(arg, default) for arg, default in zip(args_with_defaults, node.args.defaults)])


                # Handle variable positional arguments (*args)
                if node.args.vararg:
                    vararg_str = f"*{node.args.vararg.arg}"
                    if node.args.vararg.annotation:
                        vararg_str += f": {ast.unparse(node.args.vararg.annotation)}"
                    header_parts.append(vararg_str)

                # Handle keyword-only arguments (**kwargs requires a leading *)
                if node.args.kwonlyargs:
                    if not node.args.vararg: # Add a '*' if there's no *args
                         header_parts.append('*')

                    kwonly_args = [format_arg_with_default(arg, default) for arg, default in zip(node.args.kwonlyargs, node.args.kw_defaults)]
                    header_parts.extend(kwonly_args)


                # Handle variable keyword arguments (**kwargs)
                if node.args.kwarg:
                    kwarg_str = f"**{node.args.kwarg.arg}"
                    if node.args.kwarg.annotation:
                        kwarg_str += f": {ast.unparse(node.args.kwarg.annotation)}"
                    header_parts.append(kwarg_str)

                # Handle return annotation
                return_annotation = f" -> {ast.unparse(node.returns)}" if node.returns else ""

                function_header = f"def {function_name}({', '.join(header_parts)}){return_annotation}:"

                # Extract docstring
                docstring = ast.get_docstring(node)

                functions_info.append((function_name, function_header, docstring))

    except Exception as e:
        print(f"Error processing file {filepath}: {e}")
        return [] # Return empty list if there's an error

    return functions_info

def process_directory(directory_path):
    """
    Walks through a directory, finds Python files (excluding test files),
    and extracts function information.

    Args:
        directory_path (str): The path to the directory to process.
    """
    if not os.path.isdir(directory_path):
        print(f"Error: Directory not found at {directory_path}")
        return

    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith(".py") and "test" not in file:
                filepath = os.path.join(root, file)
                print(f"Processing file: {filepath}")
                function_details = extract_function_info(filepath)
                if function_details:
                    for name, header, docstring in function_details:
                        print(f"  Function Name: {name}")
                        print(f"  Function Header: {header}")
                        print(f"  Docstring:")
                        if docstring:
                            print(f"    '''\n{docstring}\n    '''")
                        else:
                            print("    No docstring found.")
                        print("-" * 20)
                else:
                    print("  No functions found or error processing file.")
                print("=" * 40)

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python your_script_name.py <directory_path>")
        sys.exit(1)

    target_directory = sys.argv[1]
    process_directory(target_directory)