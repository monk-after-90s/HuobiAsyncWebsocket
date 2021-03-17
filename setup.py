import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="HuobiAsyncWebsocket",
    version="1.1.2",
    author="Antas",
    author_email="",
    description="Asynchronous huobi websocket SDK",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/monk-after-90s/HuobiAsyncWebsocket.git',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
