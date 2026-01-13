import services.pipelineDriver as pipelinedriver
import sys
import logger as logging

def main():
    try:
        pipelinedriver.startOfPipeLine()
    except Exception as e:
        sys.exit(1)

if __name__ == '__main__':
    main()